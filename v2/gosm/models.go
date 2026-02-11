package gosm

type GetSecretResponse struct {
	ARN       string `json:"arn"`
	Name      string `json:"name"`
	Secret    Secret `json:"secret"`
	IsKeyPair bool   `json:"is_key_pair"`
}

type Secret struct {
	Key   *string `json:"key"`
	Value string  `json:"value"`
}
