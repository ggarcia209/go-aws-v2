package gosm

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	sm "github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager/types"
	"github.com/ggarcia209/go-aws-v2/v2/goaws"
)

//go:generate mockgen -destination=../mocks/gosmmock/secrets_manager.go -package=gosmmock . SecretsManagerLogic
type SecretsManagerLogic interface {
	GetSecret(ctx context.Context, key string) (*GetSecretResponse, error)
}

// SecretsManagerClientAPI defines the interface for the AWS SecretsManager client methods used by this package.
//
//go:generate mockgen -destination=./secrets_manager_client_test.go -package=gosm . SecretsManagerClientAPI
type SecretsManagerClientAPI interface {
	GetSecretValue(ctx context.Context, params *sm.GetSecretValueInput, optFns ...func(*sm.Options)) (*sm.GetSecretValueOutput, error)
}

type SecretsManager struct {
	svc SecretsManagerClientAPI
}

func NewSecretsManager(config goaws.AwsConfig) *SecretsManager {
	client := sm.NewFromConfig(config.Config)
	return &SecretsManager{
		svc: client,
	}
}

// GetSecret returns the secret at the given key.
func (s *SecretsManager) GetSecret(ctx context.Context, key string) (*GetSecretResponse, error) {
	input := &sm.GetSecretValueInput{
		SecretId: aws.String(key),
	}

	secret, err := s.svc.GetSecretValue(ctx, input)
	if err != nil {
		var notExist *types.ResourceNotFoundException
		var policy *types.PublicPolicyException
		var re *awshttp.ResponseError
		switch {
		case errors.As(err, &notExist):
			return nil, NewSecretNotFoundError(key)
		case errors.As(err, &policy):
			return nil, NewSecretPermissionsError(key)
		case errors.As(err, &re):
			if re.ResponseError == nil {
				return nil, fmt.Errorf("s.svc.GetSecretValue: %w", re.Err)
			}
			switch re.ResponseError.HTTPStatusCode() {
			case http.StatusUnauthorized,
				http.StatusForbidden:
				return nil, NewSecretPermissionsError(key)
			case http.StatusNotFound:
				return nil, NewSecretNotFoundError(key)
			default:
				return nil, goaws.NewInternalError(fmt.Errorf("s.svc.GetSecretValue: %w", re.Err))
			}
		default:
			return nil, goaws.NewInternalError(fmt.Errorf("s.svc.GetSecretValue: %w", err))
		}
	}

	return &GetSecretResponse{Secret: string(secret.SecretBinary)}, nil
}
