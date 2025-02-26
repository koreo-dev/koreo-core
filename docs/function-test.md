# FunctionTest

Koreo provides `FunctionTest` to make validating the behavior of function
control loops easier. `FunctionTest` provides a direct means of simulating
changing inputs and external state between iterations. It includes built-in
contract testing in addition to return value testing. This allows for the
testing of the full life cycle, including error handling.

Within a `FunctionTest`, inputs and an initial state may be provided along with
a set of test cases. The test cases are run sequentially so that changing
conditions may be precisely simulated and assertions about the behavior made.
Mutations to the resource or inputs (by the function or test setup) are
preserved between each test case, allowing for realistic testing without tons
of complex setup. To make testing more robust, `variant` tests do not preserve
mutations across tests. This allows for testing conditions that may cause
errors, or easily testing other variant behaviors.



| Full Specification         | Description           |
| :--------------------------| :-------------------- |
| **`apiVersion`**: `koreo.dev/v1beta1` | Specification version |
| **`kind`**: `FunctionTest` | Always `FunctionTest` |
| **`metadata`**:            | |
| **`  name`**:              | Name of the `FunctionTest`|
| **`  namespace`**:         | Namespace |
| **`spec`**:                | |
| **`  functionRef`**:       | The Function Under Test. |
| **`    kind`**:            | The kind of function being tested (`ValueFunction` or `ResourceFunction`) |
| **`    name`**:            | Name of the Function to test. |
|  *`  inputs`*:             | If the function needs inputs, all required inputs must be provided for the base case. |
| **`    {}`**               | If `inputs` is specified, must be an object. |
|  *`  currentResource`*:    | _Optional_ An initial resource state may be provided. This will be provided to the first `testCase`. |
| **`    {}`**               | If `currentResource` is specified, this must be an object.
| **`  testCases`**:         | To correctly model the control loop, test cases run sequentially. Up to 20 may be specified in a single test. Each test builds off the prior non-variant test case's mutations. |
|  *`  - label`*:            | _Optional_ Descriptive name for the test case. If not provided, the (1-indexed) test number will be used. |
|  *`    variant`*:          | _Optional_ Variant test state mutations do not carry forward. This allows testing of error cases, bad inputs, resource error conditions, and variant behaviors of the function. Defaults to `false`. |
|  *`    skip`*:             | _Optional_ Skip running the test. Defaults to `false` |
|  *`    inputOverrides`*:   | _Optional_ Input values that will _replace_ the base inputs, allowing for the testing of changing inputs. If not a `variant` test case, the input changes will carry forward. |
| **`      {}`**             | When specified, must be an object. These will _replace_ the prior input values. |
|  *`    currentResource`*:  | _Optional_ Entirely replace the current resource. If not a `variant` test case, this will carry forward. |
| **`      {}`**             | When specified, must be an object. It will fully replace the cluster view of the resource. |
|  *`    overlayResource`*:  | _Optional_ Partially update values on the resource. This is an _overlay_, so partial updates are possible. If not a `variant` test case, this will carry forward. |
| **`      {}`**             | When specified, must be an object. This will be overlaid on top of the current resource view. This allows for simulating controller behaviors such as updating values or conditions. |
|  *`    expectResource`*:   | Assert that a resource mutation was made. This assertion will fail if a resource create or patch was not attempted. |
| **`      {}`**             | The expected object should be a full and complete view of the expected object. By default, and exact comparison is made. See comparison directives for alterations. |
|  *`    expectDelete`*:     | Assert that the resource was deleted. This is used for testing _recreate_ updates. |
|  *`    expectReturn`*:     | Assert return value state. |
| **`      {}`**             | When specified, must be an object. An exact comparison is made. |
|  *`    expectOutcome`*:    | Assert return _types_. This is useful for error handling, or testing create / return flows without making specific assertions. |
|  *`      ok`*: **`{}`**    | Assert that the function ran successfully without resource modifications. The value must be the empty object (`{}`) |
|  *`      skip`*:           | Assert that the function returned a `Skip`. |
| **`        message`**:     | Assert the `Skip`'s message contains this value using a case-insensitive `in` comparison. The empty string may be used to match any value. |
|  *`      depSkip`*:        | Assert that the function returned a `DepSkip` |
| **`        message`**:     | Assert the `DepSkip`'s message contains this value using a case-insensitive `in` comparison. The empty string may be used to match any value. |
|  *`      retry`*:          | Assert that the function returned a `Retry`. This may be an explicit retry _or_ due to any resource modifications, including create, patch, and delete. |
| **`        message`**:     | Assert the `Retry`'s message contains this value using a case-insensitive `in` comparison. The empty string may be used to match any value. |
| **`        delay`**:       | Assert the `Retry`'s delay is exactly this value; `0` may be used to match any value. |
| **`      permFail`**:      | Assert that the function returned a `PermFail` |
| **`        message`**:     | Assert the `PermFail`'s message contains this value using a case-insensitive `in` comparison. The empty string may be used to match any value. |

## Usage

The following sections elaborate on the key features of `FunctionTest` and
their intended uses.

### `spec.functionRef`: Define the Function Under Test

Specify the function to be tested. Functions define a control-loop, and hence
are executed many times. In order to make testing easier, and far less
repetitive, the function will be evaluated once per test case. Any mutations
the function makes will be carried forward to the next test case _unless_
variant is specified. This allows for testing the function in a realistic
manner and makes detecting conditions such as update-loops possible.

### `spec.inputs`: Base inputs

If a function requires input values, they should be fully specified for the
base case. To test bad-input cases, make use of `inputOverrides` within a test
case. This makes testing both specific variants and the "happy path" case
easier and more reliable.

### `spec.currentResource`: Preexisting Resource Tests

If you would like to test creation, do not specify `spec.currentResource`.
Instead omit it. Once it has been created by the first (non-variant) test case,
it will be available to subsequent test cases.

However, for some tests it is desirable to specify a base resource state, then
mutate it within test cases (using `overlayResource`). This is especially
useful when combined with `variant` so that various conditions may be tests,
such as spec changes or conditions the managed resource's controller may make
or set. It makes it very easy to test many variant cases without a lot of
boilerplate.

May not be specified for `ValueFunctions`.

### Modeling Reconciliation

Each item in the `spec.testCases` array defines a test case to be run. They are
run sequentially so that you may correctly model the executions of the function
over time. `ValueFunctions` are pure—there is no external interaction or state—
so the tests are effectively _unit tests_. `ResourceFunctions` are far more
complex because they interact with external state in multiple ways. There are
two particularly useful approaches to structuring `ResourceFunction` test
flows, discussed below.

#### Happy Path Foundation

Model the happy-path flow by testing creation and then that the expected return
value is correct. Next, add test cases (using `inputOverrides` or
`overlayResource` to update state) to test update (patch or recreate) cases and
ensure they behave as desired. The resource should always come back to a steady
state; you may use an `expectOutcome` with an `ok: {}` assertion to validate
steady state.

Once the happy-path reconciliation flow is written, tested, and working well,
add in _variant_ tests to ensure that if some condition changes it is handled
as desired. For instance, if the resource enters an error state is it updated
or does the function correctly return an error condition? Using _variant_
tests, you may safely insert these tests within the happy-path flow.


#### Base with Variants

Specify a starting point with good `spec.inputs` values. For creation or
precondition checks, omit specifying `spec.currentResource`. For update or post
condition tests, specify `spec.currentResource`. Generally a good state, in
stable condition is preferable to ensure each test is validating the correct
behavior. One the base state is defined, add test cases (using `inputOverrides`
or `overlayResource`) to simulate various inputs, conditions, errors, or
external resource changes to ensure they are correctly handled. Often it is
useful to make these test cases _variant_, so that errors do not compound or
conflate across test cases.

This approach is particularly helpful for functions requiring complex error
handling, with lots of pre or post condition checks, or with very involved
return values. It allows for validating lots of cases with minimal boilerplate
required.


### `spec.testCases`: Defining a Test Case

An optional `label` may be specified to help you identify or understand the
intention of the test case. The label is used within the test report. If
omitted the (1-indexed) position is used.

A test case may be skipped by setting `skip` to `true`. Keep in mind that if
the test case was mutating state, this may break subsequent tests.

Preserving state mutations across tests is not always desirable. In order to
discard any mutations (either test case setup or return values), set `variant`
to `true`. This instructs the test runner to ignore any state mutations outside
the scope of the _variant_ test case.

In order to simulate bad inputs, changing inputs, or different behaviors,
`inputOverrides` may be used to _replace_ input values. This can be useful to
test preconditions, but also for ensuring the return value or resource matches
expectations for various inputs.

In order to test behavior with different current resource states, there are two
options available. To simulate external controller (or user) modifications by
updating specific fields, replacing specific values, or adding status
conditions, `overlayResource` should be used. This is very useful for
simulating interactions with a controller that is reporting back status
information. Alternatively, to _fully replace_ the current resource,
`currentResource` may be used. The resource must be specified in its entirety.

#### `expectResource`: Resource Mutation Assertion

When resource mutations are expected `expectResource` may be used to validate
that the resource exactly matches a Target Resource Specification. The full
resource should be provided, and will be compared exactly. If no resource
modifications (create or update) are attempted, an `expectResource` assertion
fails.

For cases where list order should be ignored or treating a list as a map is
required, you may use the compare directives to alter the resource validation.
These are not typically required within tests, but are sometimes helpful.

    x-koreo-compare-as-set
    x-koreo-compare-as-map

The directives behave as describes within the `ResourceFunction` documentation.
Place them within the `expectResource` body, just as for the Target Resource
Specification.

May not be specified for `ValueFunctions`.

#### `expectDelete`: Resource Recreate

When making use of `update.recreate` behavior, the resource will be deleted
if differences are detected. Use `expectDelete` in order to assert that the
difference is detected and the resource deleted.

If this is not a _variant_ test case, the next test case will create the
resource.

May not be specified for `ValueFunctions`.

#### `expectReturn`: Return Value Testing

Return values may be tested using `expectReturn`. This is an exact match
comparison. If any resource modifications are attempted, an `expectReturn`
assertion fails.

#### `expectOutcome`: Return Type Testing

In many cases it is useful to test the return _type_ of a function, for
instance when validating pre or post conditions that might return skips or
errors.

Structurally, `expectOutcome` is similar to `preconditions` and
`preconditions`. 

Because `ok` has a dedicated return value test (`expectReturn`), its
`expectOutcome` test is used to assert that the function succeeded
without testing anything specific about its return value.

For all other outcome tests, a `message` assertion is required. The outcome's
message must _contain_ the asserted value. It is not an equality but a
case-insensitive, contains test. This is to make assertions easier to author
and less fragile, while still enabling you to test for specific outcomes.

The only other unique case is `retry`, which also requires a `delay` assertion.
This is an _exact_ match. If you do not care about the specific `delay` time, 0
will match any value.


## Example FunctionTest

In order to demonstrate `FunctionTest`, we will test a simple but
representative `ResourceFunction`.
```yaml
apiVersion: koreo.dev/v1beta1
kind: FunctionTest
metadata:
  name: function-test-demo.v1
  namespace: koreo-demo
spec:
  functionRef:
    kind: ResourceFunction
    name: function-test-demo.v1

  # Provde base, good inputs.
  inputs:
    metadata:
      name: test-demo
      namespace: tests
    enabled: true
    int: 64

  # Each testCase is an iteration of the control loop.
  testCases:
  # The first pass through creates the resource, and we can verify that it
  # matches our expections
  - label: Initial Create
    expectResource:
      apiVersion: koreo.dev/v1beta1
      kind: TestDummy
      metadata:
        name: test-demo
        namespace: tests
      spec:
        value: 64
        doubled: 128
        listed:
        - 65
        - 66

  # The resource from the first test is now `currentResource`. We can ensure
  # that the function waits until the ready condition is met.
  - label: Retry until ready
    expectOutcome:
      retry:
        # We aren't concerned with the specific delay.
        delay: 0
        # Make sure the message explains the issue.
        message: not ready

  # We can simulate some external update, such as a controller, setting a
  # status value.
  - label: Test ready state
    overlayResource:
      status:
        ready: true
    expectOutcome:
      ok: {}

  # If we do not want to mutate the overall test state, we can test variant
  # cases.
  - variant: true
    label: Un-ready state
    overlayResource:
      status:
        ready: false
    expectOutcome:
      retry:
        delay: 0
        message: ''

  # Because the prior test was a `variant` case, the overall state is still Ok.
  - label: Test ready state
    expectReturn:
      bigInt: 6400
      ready: true
      ref:
        apiVersion: koreo.dev/v1beta1
        kind: TestDummy
        name: test-demo
        namespace: tests

  # In order to test patch updates, re-check the resource.
  - label: Update
    inputOverrides:
      int: 22
    expectResource:
      apiVersion: koreo.dev/v1beta1
      kind: TestDummy
      metadata:
        name: test-demo
        namespace: tests
      spec:
        value: 22
        doubled: 44
        listed:
        - 23
        - 24
      # We need to check this now, because we added it to the resource state so
      # it will carry forward.
      status:
        ready: true

  # We can simulate a full replacement of the resource and ensure it is patched.
  - label: Resource Replacement
    currentResource:
      apiVersion: koreo.dev/v1beta1
      kind: TestDummy
      metadata:
        name: test-demo
        namespace: tests
      spec:
        value: 1
        doubled: 2
        listed:
        - 3
        - 4
    expectResource:
      apiVersion: koreo.dev/v1beta1
      kind: TestDummy
      metadata:
        name: test-demo
        namespace: tests
      spec:
        value: 22
        doubled: 44
        listed:
        - 23
        - 24

  # Now the resource should be stable again, if status is ready.
  - label: Test ready state
    overlayResource:
      status:
        ready: true
    expectOutcome:
      ok: {}
---
apiVersion: koreo.dev/v1beta1
kind: ResourceFunction
metadata:
  name: function-test-demo.v1
  namespace: koreo-demo
spec:
  preconditions:
  - assert: =inputs.int > 0
    permFail:
      message: ="`int` must be positive, received '" + string(inputs.int) + "'"

  - assert: =inputs.enabled
    skip:
      message: User disabled the ResourceFunction

  apiConfig:
    apiVersion: koreo.dev/v1beta1
    kind: TestDummy
    plural: testdummies

    name: =inputs.metadata.name
    namespace: =inputs.metadata.namespace

  resource:
    metadata: =inputs.metadata
    spec:
      value: =inputs.int
      doubled: =inputs.int * 2
      listed:
      - =inputs.int + 1
      - =inputs.int + 2

  postconditions:
    # Note, you must explicitly handle cases where the value might not be
    # present.
  - assert: =has(resource.status.ready) && resource.status.ready
    retry:
      message: Not ready yet
      delay: 5

  return:
    ref: =resource.self_ref()
    bigInt: =inputs.int * 100
    ready: '=has(resource.status.ready) ? resource.status.ready : "not ready"'

```
