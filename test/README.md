# Scale Test Suite
This suite is designed so anyone can add a test suite for scale testing on a DC/OS cluster.

## Quick Start
### Add New Scale Test
**Add New Test Suite Package**
All component-specific tests are located in the `test/suite` package of this project. Add a directory (go package) in this directory for your component. Let's use example `foo`.

`mkdir test/suite/foo`

**Define a type that implements `ScaleTester{}`**
Next, define a struct for `FooTest` that implements the `ScaleTester{}` interface:
```
package foo

type FooTest struct {
	FooRate int
	BarConfig string
}

func (f FooTest) Run(supervisorURL string) error {}

func (f FooTest) GetEvent() chan backend.BigQuerySchema {}

func (f FooTest) GetContext() context.Context {}
```
The `GetEvent()` and `GetContext()` functions are convienience for you, they're not used explicity by the top level `test` package. The important thing you get right, is the `Run()` method. This is called by the top level package to run your test, and is passed the URL to the supervisor process.

Your test is responsible for sending events to the supervisor process, at this URL. See the API documentation for supervisor in this project under the top level `supervisor` package. 

**Implement a `.NewTestSuite()` Method**
It's expected that your package will have a `NewTestSuite` method. Though not explicity called by the test package, this method naming convention keeps the expected behavior of the packages within this project homogenized and is a good practice.

`func NewTestSuite(fooRate int, BarConfig string) (FooTest, error) { ... }`

**Add a Test Suite Command**
Once `Foo` test is ready, we can add a command for it under `test/cmd` directory. 
```
mkdir test/cmd/foo-test/
touch test/cmd/foo-test/main.go
```
In the `main.go` let's call our new test suite and a top level `TestSuite` object:

```
package main

import (
	"github.com/mesosphere/gravity/test"
	"github.com/mesosphere/gravity/test/suite/foo"
)

var (
	fooRate := flag.Int("foo-rate", 5, "The rate of foo")
	barConfig := flag.String("bar-config", "baz", "The config for bar")
)

func main() {
	flag.Parse()

	fooSuite, err := foo.NewTestSuite(*fooRate, *barConfig)
	if err != nil {
		panic(err)
	}

	testSuite, err := test.NewSuite(
		test.OptionSupervisorURL(*supervisorURL),
		test.OptionTesters(fooSuite))
	
	if err != nil {
		panic(err)	
	}

	testSuite.Start() 
}	 
```
The `.Start()` call will execute all `ScaleTester`'s passed to the `.OptionTesters()` option. It must be called to start tests, which it does via `.Run()` interface call.

**Add Another `TestSuite` to Command**
You can add as many test suites to the scale test command. If for example you want your command to run in conjunction with the `journald` test suite, you simply add it to the `OptionTesters`:

```
	import "github.com/gravity/test/suite/journald"

	journaldSuite, err := journald.NewTestSuite(options...)
	if err ...
	
	testSuite, err := test.NewSuite(
		test.OptionTesters(fooSuite, journaldSuite))
	if err ...

	testSuite.Start()
```

## Big Queries for Big Query
*Show journald load for particular host by timestamp in descending time:*
```
SELECT *
FROM [massive-bliss-781:dcos_performance2.mnaboka] 
WHERE Hostname CONTAINS "bohr" AND Name CONTAINS "journal"
Order By Timestamp DESC 
LIMIT 1000
```
