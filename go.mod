module github.com/jayalane/aws-learn

go 1.24.5

require (
	github.com/aws/aws-sdk-go v1.55.8
	github.com/jayalane/go-counter v0.0.0-20241122060713-a345f1a308be
	github.com/jayalane/go-persist-set v1.0.0
	github.com/jayalane/go-tinyconfig v0.0.0-20251108040520-8acfb20dfa57
)

require github.com/jmespath/go-jmespath v0.4.0 // indirect

replace github.com/jayalane/go-counter => ../go-counter
