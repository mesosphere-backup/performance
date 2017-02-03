package test

// TestOption represents a test option for the scale test
// package
type TestOption func(*ScaleTest) error

// OptionSupervisorURL configures the URL to the supervisor
// process which proxies Big Query data transmission
func OptionSupervisorURL(url string) TestOption {
	return func(s *ScaleTest) error {
		s.SupervisorURL = url
		return nil
	}
}

// OptionTesters configures the test suites to run
// during the scale test
func OptionTesters(testers ...ScaleTester) TestOption {
	return func(s *ScaleTest) error {
		s.Testers = testers
		return nil
	}
}
