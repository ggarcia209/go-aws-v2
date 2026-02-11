package gosm

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	sm "github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager/types"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/ggarcia209/go-aws-v2/v2/goaws"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	gomock "go.uber.org/mock/gomock"
)

func TestNewSecretsManager(t *testing.T) {
	cfg, err := goaws.NewDefaultConfig(context.Background())
	if err != nil {
		t.Errorf("goaws.NewDefaultConfig: %v", err)
		return
	}

	require.NotNil(t, cfg)

	// test interface implementation
	client := NewSecretsManager(*cfg)
	assert.NotNil(t, client)
	assert.NotNil(t, client.svc)
	assert.Implements(t, (*SecretsManagerLogic)(nil), client)
}

func TestSecretsManager_GetSecret(t *testing.T) {
	tests := []struct {
		name           string
		key            string
		mockSetup      func(ctrl *gomock.Controller) SecretsManagerClientAPI
		expectedSecret string
		expectedError  error
	}{
		{
			name: "Success -- plain text",
			key:  "test-secret-key",
			mockSetup: func(ctrl *gomock.Controller) SecretsManagerClientAPI {
				mockSvc := NewMockSecretsManagerClientAPI(ctrl)
				mockSvc.EXPECT().GetSecretValue(gomock.Any(), &sm.GetSecretValueInput{
					SecretId: aws.String("test-secret-key"),
				}).Return(&sm.GetSecretValueOutput{
					ARN:          aws.String("test-secret-arn"),
					Name:         aws.String("test-secret-name"),
					SecretString: aws.String("test-secret-value"),
					SecretBinary: []byte("test-secret-value"),
				}, nil).Times(1)
				return mockSvc
			},
			expectedSecret: "test-secret-value",
			expectedError:  nil,
		},
		{
			name: "Success -- k / v pair",
			key:  "test-secret-key",
			mockSetup: func(ctrl *gomock.Controller) SecretsManagerClientAPI {
				mockSvc := NewMockSecretsManagerClientAPI(ctrl)
				mockSvc.EXPECT().GetSecretValue(gomock.Any(), &sm.GetSecretValueInput{
					SecretId: aws.String("test-secret-key"),
				}).Return(&sm.GetSecretValueOutput{
					ARN:          aws.String("test-secret-arn"),
					Name:         aws.String("test-secret-name"),
					SecretString: aws.String(`{"test-secret-key": "test-secret-value"}`),
				}, nil).Times(1)
				return mockSvc
			},
			expectedSecret: "test-secret-value",
			expectedError:  nil,
		},
		{
			name: "Success -- binary only",
			key:  "test-secret-key",
			mockSetup: func(ctrl *gomock.Controller) SecretsManagerClientAPI {
				mockSvc := NewMockSecretsManagerClientAPI(ctrl)
				mockSvc.EXPECT().GetSecretValue(gomock.Any(), &sm.GetSecretValueInput{
					SecretId: aws.String("test-secret-key"),
				}).Return(&sm.GetSecretValueOutput{
					ARN:          aws.String("test-secret-arn"),
					Name:         aws.String("test-secret-name"),
					SecretBinary: []byte(`{"test-secret-key": "test-secret-value"}`),
				}, nil).Times(1)
				return mockSvc
			},
			expectedSecret: "test-secret-value",
			expectedError:  nil,
		},
		{
			name: "error - missing arn",
			key:  "test-secret-key",
			mockSetup: func(ctrl *gomock.Controller) SecretsManagerClientAPI {
				mockSvc := NewMockSecretsManagerClientAPI(ctrl)
				mockSvc.EXPECT().GetSecretValue(gomock.Any(), &sm.GetSecretValueInput{
					SecretId: aws.String("test-secret-key"),
				}).Return(&sm.GetSecretValueOutput{
					Name:         aws.String("test-secret-name"),
					SecretBinary: []byte(`{"test-secret-key": "test-secret-value"}`),
				}, nil).Times(1)
				return mockSvc
			},
			expectedError: NewMissingResponseDataError("ARN"),
		},
		{
			name: "error - missing name",
			key:  "test-secret-key",
			mockSetup: func(ctrl *gomock.Controller) SecretsManagerClientAPI {
				mockSvc := NewMockSecretsManagerClientAPI(ctrl)
				mockSvc.EXPECT().GetSecretValue(gomock.Any(), &sm.GetSecretValueInput{
					SecretId: aws.String("test-secret-key"),
				}).Return(&sm.GetSecretValueOutput{
					ARN:          aws.String("test-secret-arn"),
					SecretBinary: []byte(`{"test-secret-key": "test-secret-value"}`),
				}, nil).Times(1)
				return mockSvc
			},
			expectedError: NewMissingResponseDataError("Name"),
		},
		{
			name: "error - missing secret",
			key:  "test-secret-key",
			mockSetup: func(ctrl *gomock.Controller) SecretsManagerClientAPI {
				mockSvc := NewMockSecretsManagerClientAPI(ctrl)
				mockSvc.EXPECT().GetSecretValue(gomock.Any(), &sm.GetSecretValueInput{
					SecretId: aws.String("test-secret-key"),
				}).Return(&sm.GetSecretValueOutput{
					ARN:  aws.String("test-secret-arn"),
					Name: aws.String("test-secret-name"),
				}, nil).Times(1)
				return mockSvc
			},
			expectedError: NewMissingResponseDataError("Secret"),
		},
		{
			name: "error - secret not found",
			key:  "missing-secret",
			mockSetup: func(ctrl *gomock.Controller) SecretsManagerClientAPI {
				mockSvc := NewMockSecretsManagerClientAPI(ctrl)
				mockSvc.EXPECT().GetSecretValue(gomock.Any(), gomock.Any()).Return(nil, &types.ResourceNotFoundException{}).Times(1)
				return mockSvc
			},
			expectedSecret: "",
			expectedError:  NewSecretNotFoundError("missing-secret"),
		},
		{
			name: "error - permissions denied (policy)",
			key:  "restricted-secret",
			mockSetup: func(ctrl *gomock.Controller) SecretsManagerClientAPI {
				mockSvc := NewMockSecretsManagerClientAPI(ctrl)
				mockSvc.EXPECT().GetSecretValue(gomock.Any(), gomock.Any()).Return(nil, &types.PublicPolicyException{}).Times(1)
				return mockSvc
			},
			expectedSecret: "",
			expectedError:  NewSecretPermissionsError("restricted-secret"),
		},
		{
			name: "error - permissions denied (403)",
			key:  "restricted-secret-403",
			mockSetup: func(ctrl *gomock.Controller) SecretsManagerClientAPI {
				mockSvc := NewMockSecretsManagerClientAPI(ctrl)
				respErr := &awshttp.ResponseError{
					ResponseError: &smithyhttp.ResponseError{
						Response: &smithyhttp.Response{
							Response: &http.Response{
								StatusCode: http.StatusForbidden,
							},
						},
					},
				}
				mockSvc.EXPECT().GetSecretValue(gomock.Any(), gomock.Any()).Return(nil, respErr).Times(1)
				return mockSvc
			},
			expectedSecret: "",
			expectedError:  NewSecretPermissionsError("restricted-secret-403"),
		},
		{
			name: "error - not found (404)",
			key:  "missing-secret-404",
			mockSetup: func(ctrl *gomock.Controller) SecretsManagerClientAPI {
				mockSvc := NewMockSecretsManagerClientAPI(ctrl)
				respErr := &awshttp.ResponseError{
					ResponseError: &smithyhttp.ResponseError{
						Response: &smithyhttp.Response{
							Response: &http.Response{
								StatusCode: http.StatusNotFound,
							},
						},
					},
				}
				mockSvc.EXPECT().GetSecretValue(gomock.Any(), gomock.Any()).Return(nil, respErr).Times(1)
				return mockSvc
			},
			expectedSecret: "",
			expectedError:  NewSecretNotFoundError("missing-secret-404"),
		},
		{
			name: "error - internal error",
			key:  "broken-secret",
			mockSetup: func(ctrl *gomock.Controller) SecretsManagerClientAPI {
				mockSvc := NewMockSecretsManagerClientAPI(ctrl)
				mockSvc.EXPECT().GetSecretValue(gomock.Any(), gomock.Any()).Return(nil, errors.New("something went wrong")).Times(1)
				return mockSvc
			},
			expectedSecret: "",
			expectedError:  goaws.NewInternalError(fmt.Errorf("s.svc.GetSecretValue: %w", errors.New("something went wrong"))),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mockSvc := tt.mockSetup(ctrl)
			s := &SecretsManager{svc: mockSvc}

			res, err := s.GetSecret(context.Background(), tt.key)

			if tt.expectedError != nil {
				require.Error(t, err)
				assert.EqualError(t, err, tt.expectedError.Error())
				assert.Implements(t, (*goaws.AwsError)(nil), err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expectedSecret, res.Secret.Value)
			}
		})
	}
}
