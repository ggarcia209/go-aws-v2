package gosm

import (
	"context"
	"encoding/json"
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
	// get secret from secrets manager
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

	// get response from secrets manager
	if secret.ARN == nil {
		return nil, NewMissingResponseDataError("ARN")
	}
	if secret.Name == nil {
		return nil, NewMissingResponseDataError("Name")
	}
	resp := &GetSecretResponse{
		ARN:  *secret.ARN,
		Name: *secret.Name,
	}

	var secretString string
	if secret.SecretString == nil && secret.SecretBinary == nil {
		return nil, NewMissingResponseDataError("Secret")
	}
	if secret.SecretString != nil {
		secretString = *secret.SecretString
	} else {
		secretString = string(secret.SecretBinary)
	}

	// first, we attempt to get secret in k/v format
	var kv = make(map[string]string)
	if err = json.Unmarshal([]byte(secretString), &kv); err != nil {
		// secret is in plain text format
		resp.Secret.Value = secretString
		resp.IsKeyPair = false
	} else {
		// secret is in k/v format
		for k, v := range kv {
			resp.Secret.Key = aws.String(k)
			resp.Secret.Value = v
		}
		resp.IsKeyPair = true
	}

	return resp, nil
}
