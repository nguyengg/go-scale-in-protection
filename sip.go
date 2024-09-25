package sip

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/ec2/imds"
	"github.com/aws/aws-sdk-go-v2/service/autoscaling"
	"io"
	"log"
	"sync"
	"time"
)

// ScaleInProtector monitors active statuses of several workers to enable or disable scale-in protection accordingly.
//
// The zero value ScaleInProtector is ready for use. StartMonitoring should be called in a separate goroutine to start
// the monitoring loop. Each worker then calls either SignalActive or SignalIdle at the appropriate time, passing the
// worker identifier as the sole argument.
type ScaleInProtector struct {
	// InstanceId is the instance Id to enable or disable scale-in protection.
	//
	// If not specified, an imds.Client created from the default aws.Config (config.LoadDefaultConfig) will be used to
	// detect the instance Id of the host. If one cannot be detected, StartMonitoring will return a non-nil error.
	InstanceId string
	// AutoScalingGroupName is the name of the Auto Scaling group that contains the instance specified by InstanceId.
	//
	// If not specified, the given AutoScaling will be used to find the Auto Scaling group that contains the instance
	// specified by InstanceId. If one cannot be found, StartMonitoring will return a non-nil error. If both InstanceId
	// and AutoScalingGroupName are given but the Auto Scaling group does not contain the instance, an error is also
	// returned (you cannot have the monitor effects an instance different from the one it's running on for safety). If
	// you want an option to disable this check, send a PR.
	AutoScalingGroupName string
	// AutoScaling is the client that will be used to make Auto Scaling service calls.
	//
	// If not given, an autoscaling.Client created from the default aws.Config (config.LoadDefaultConfig) will be used.
	AutoScaling AutoScalingAPIClient
	// IdleAtLeast specifies the amount of time all workers must have been idle before scale-in protection may be
	// disabled.
	//
	// The delay starts from when the last worker becomes idle. If you want to measure from the moment the first worker
	// becomes idle, send a PR (trailing vs. leading delay).
	IdleAtLeast time.Duration
	// Logger is used to log whenever the scale-in protection changes.
	//
	// Defaults to log.Default.
	Logger *log.Logger

	mu        sync.Mutex
	active    map[string]bool
	protected bool
	started   bool

	ach chan string
	ich chan string
}

// AutoScalingAPIClient extracts the subset of autoscaling.Client APIs that ScaleInProtector uses.
type AutoScalingAPIClient interface {
	DescribeAutoScalingInstances(context.Context, *autoscaling.DescribeAutoScalingInstancesInput, ...func(*autoscaling.Options)) (*autoscaling.DescribeAutoScalingInstancesOutput, error)
	SetInstanceProtection(context.Context, *autoscaling.SetInstanceProtectionInput, ...func(*autoscaling.Options)) (*autoscaling.SetInstanceProtectionOutput, error)
}

// StartMonitoring starts the monitoring loop.
//
// The method should be called in a separate goroutine because it will not return until the given context is cancelled;
// [context.Context.Err] is always returned in this case.
//
// The method panics if it has been called more than once.
func (s *ScaleInProtector) StartMonitoring(ctx context.Context) (err error) {
	if err = s.init(ctx); err != nil {
		return err
	}

	var delay <-chan time.Time

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-s.ach:
			// enabling scale-in protection takes place right away.
			if err = s.toggle(ctx, true); err != nil {
				return err
			}
		case <-s.ich:
			if len(s.active) > 0 || !s.protected {
				continue
			}

			// for simplicity, always use delay to effect disabling of scale-in protection.
			if delay = time.After(max(0, s.IdleAtLeast)); s.IdleAtLeast > 0 {
				s.Logger.Printf("all workers idle, will disable scale-in protection at %s (in %.4f seconds)", time.Now().Add(s.IdleAtLeast).Format(time.RFC3339), s.IdleAtLeast.Seconds())
			}
		case <-delay:
			// s.ach and s.ich only has values while s.mu is locked but not delay so must do its own locking here.
			s.mu.Lock()
			err = s.toggle(ctx, false)
			s.mu.Unlock()
			if err != nil {
				return err
			}

			delay = nil
		}
	}
}

// IsProtectedFromScaleIn returns the internal flag reflecting whether scale-in protection is enabled or not.
//
// It is entirely possible for the monitor to think it has scale-in protection enabled while an external action may have
// disabled it and vice versa.
func (s *ScaleInProtector) IsProtectedFromScaleIn() bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.protected
}

// SignalActive should be called by a worker passing its identifier when it has an active job.
func (s *ScaleInProtector) SignalActive(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.active[id] = true

	select {
	case s.ach <- id:
	default:
		// in case StartMonitoring has not been called, s.ach will be nil.
	}
}

// SignalIdle should be called by a worker passing its identifier when it has become idle.
func (s *ScaleInProtector) SignalIdle(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.active, id)

	select {
	case s.ich <- id:
	default:
		// in case StartMonitoring has not been called, s.ich will be nil.
	}
}

func (s *ScaleInProtector) init(ctx context.Context) error {
	s.mu.Lock()
	started := s.started
	s.mu.Unlock()
	if started {
		panic("monitor has already been started once")
	}

	if s.Logger == nil {
		s.Logger = log.Default()
	}

	if s.IdleAtLeast < 0 {
		return fmt.Errorf("cannot specify negative IdleAtLeast: %d", s.IdleAtLeast)
	}

	if s.InstanceId == "" {
		cfg, err := config.LoadDefaultConfig(ctx)
		if err != nil {
			return fmt.Errorf("create default config error: %w", err)
		}

		imdsClient := imds.NewFromConfig(cfg)
		if output, err := imdsClient.GetMetadata(ctx, &imds.GetMetadataInput{Path: "instance-id"}); err != nil {
			return fmt.Errorf("get IMDS metadata error: %w", err)
		} else if data, err := io.ReadAll(output.Content); err != nil {
			return fmt.Errorf("read IMDS metadata error: %w", err)
		} else {
			s.InstanceId = string(data)
		}
	}

	if s.AutoScaling == nil {
		cfg, err := config.LoadDefaultConfig(ctx)
		if err != nil {
			return fmt.Errorf("create default config error: %w", err)
		}

		s.AutoScaling = autoscaling.NewFromConfig(cfg)
	}

	if output, err := s.AutoScaling.DescribeAutoScalingInstances(ctx, &autoscaling.DescribeAutoScalingInstancesInput{
		InstanceIds: []string{s.InstanceId},
	}); err != nil {
		return fmt.Errorf("describe Auto Scaling instances error: %w", err)
	} else {
		for _, instance := range output.AutoScalingInstances {
			if s.InstanceId == aws.ToString(instance.InstanceId) {
				if s.AutoScalingGroupName != "" && s.AutoScalingGroupName != aws.ToString(instance.AutoScalingGroupName) {
					return fmt.Errorf("mismatched Auto Scaling group name; given %s but instance actually belongs to %s", s.AutoScaling, aws.ToString(instance.AutoScalingGroupName))
				}
				s.AutoScalingGroupName = aws.ToString(instance.AutoScalingGroupName)
				s.protected = aws.ToBool(instance.ProtectedFromScaleIn)
				break
			}
		}

		if s.AutoScalingGroupName == "" {
			return fmt.Errorf("instance is detached from any Auto Scaling group")
		}
	}

	s.ach = make(chan string)
	s.ich = make(chan string)

	return nil
}

func (s *ScaleInProtector) toggle(ctx context.Context, protected bool) error {
	if s.protected == protected {
		return nil
	}

	s.Logger.Printf("setting scale-in protection to %t", protected)
	if _, err := s.AutoScaling.SetInstanceProtection(ctx, &autoscaling.SetInstanceProtectionInput{
		AutoScalingGroupName: &s.AutoScalingGroupName,
		InstanceIds:          []string{s.InstanceId},
		ProtectedFromScaleIn: &protected,
	}); err != nil {
		return fmt.Errorf("set scale-in protection to %t error: %w", protected, err)
	}

	s.protected = protected
	return nil
}
