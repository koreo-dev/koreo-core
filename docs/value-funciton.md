# ValueFunction

`ValueFunctions` provide a means of validation, computation, and data
reshaping. There are three common use cases for `ValueFunction`:

1. Validating data and building "known-good" structures to be provided to other
   functions.
1. Computing data structures, such as metadata, to overlay onto other resources
   in order to standardize them.
1. Validating or reshaping return values into a structure that is more
   convenient to use in other locations within a `Workflow`.

Though `ValueFunction` is a very simple construct, they are a powerful means of
reshaping or building data structures such as common labels, entire metadata
blocks, or default values for use within other Functions or Workflows.


| Full Specification            | Description           |
| :-----------------------------| :-------------------- |
| **`apiVersion`**: `koreo.dev/v1beta1` | Specification version |
| **`kind`**: `ValueFunction`   | Always `ValueFunction` |
| **`metadata`**:               | |
| **`  name`**:                 | Name of the `ValueFunction`|
| **`  namespace`**:            | Namespace |
| **`spec`**:                   | |
|  *`  preconditions`*:         | A set of assertions to determine if this function can and should be run, and if not the function's return-type. These are run in order and the first failure is returned. Exactly one return type is required. |
| **`    assert`**:             | Must be a Koreo Expression specifying an assertion, if the assertion is false the function evaluates to the specified result. |
|  *`    defaultReturn`*:       | Indicate that no further conditions should be checked, return `Ok` with the static value specified. |
| **`      {}`**                | The return value must be an object, but may be the empty object. |
|  *`    skip`*:                | Return with a `Skip` status. This is useful for functions which optionally run based on input values. |
| **`      message`**:          | The message to be returned is used for condition / status reporting. Make it descriptive of the reason for the return value. |
|  *`    depSkip`*:             | Return with a `DepSkip` status, indicating that a dependency is not ready. |
| **`      message`**:          | The message to be returned is used for condition / status reporting. Make it descriptive of the reason for the return value. |
|  *`    retry`*:               | Return a `Retry` (Wait) which will cause the `Workflow` to re-reconcile after `delay` seconds. |
| **`      message`**:          | The message to be returned is used for condition / status reporting. Make it descriptive of the reason for the return value. |
| **`      delay`**:            | Seconds to wait before re-reconciliation is attempted. |
|  *`    permFail`*:            | Return a `PermFail` which will cause the `Workflow` not to re-reconcile until the parent has been updated. This is for fatal, unrecoverable errors that will require human intervention. |
| **`      message`**:          | The message to be returned is used for condition / status reporting. Make it descriptive of the reason for the return value. |
|  *`  locals`*:                | Locals allows for defining constant values or for interim calculations. Must be an object. The values may be accessed using `locals` within the `return` block. |
|  *`  return`*:                | The return value for the function. It must be an object. |

## Usage

The following sections explain the key features of `ValueFunction` and their
intended uses.

### `spec.preconditions`: Performing Validation

It is important to check preconditions in order to determine if it is possible
to evaluate a function. For instance, it might be important to check that a
number falls within an allowed range, or a that a string meets requirements
such as length or only contains allowed characters. `spec.preconditions` allows
conditions to be _asserted_, and if the assertion fails then the function will
return a specified outcome.

You may leverage a `ValueFunction` purely to run its `spec.preconditions`. This
can be helpful to cause a `Workflow` to `Retry` or `PermFail` due to some
condition. Note that in order to block other steps, they should express a
dependency on the `ValueFunction` via their inputs—otherwise those steps will
run.

### `spec.locals`: Interim values

Because Koreo Expressions are often used to extract values or reshape data
structures, they can be rather long. `spec.locals` provides a means of naming
expressions, which can improve readability of the return value expression.

`spec.locals` is also useful for defining constant values, which may be complex
structures, such as lists or objects, or simple values. Locals are used to help
construct the return value, used within Koreo Expressions, or directly
returned.


> 📘 Note
>
> Currently, `spec.locals` may not reference other `locals`.


### `spec.return`: Returned value

The primary use cases of `ValueFunction` is to reshape or compute a return
value expression. The return expression must be an object. The keys of the
object may be constant values, data structures, or Koreo Expressions which
reference inputs (`inputs.`) or locals (`locals`).


## Example ValueFunction

The following `ValueFunction` demonstrates some of the capabilities.

```yaml
apiVersion: koreo.dev/v1beta1
kind: ValueFunction
metadata:
  name: simple-example.v1
  namespace: koreo-demo
spec:

  # Checking input values are within range or ensuring that a config is enabled
  # are common needs, preconditions support both use cases.
  preconditions:
  - assert: =inputs.values.int > 0
    permFail:
      message: ="The int input value must be positive, received '" + string(inputs.values.int) + "'"

  - assert: =inputs.enabled
    skip:
      message: User disabled the ValueFunction

  # Locals are especially useful for interim expressions to improve
  # readability, make complex expressions more ergonomic to write, or for
  # defining constant values for use within the return expression.
  locals:
    computedValues:
      halfed: =inputs.values.int / 2
      doubled: =inputs.values.int * 2

    constantList: [NORTH, SOUTH, EAST, WEST]

  # The return value of a ValueFunction must be an object, Koreo Expressions
  # have access to the `spec.locals` values.
  return:
    allowedRange:
      lower: =locals.computedValues.halfed
      upper: =locals.computedValues.doubled

    lowerWords: =locals.constantList.map(word, word.lower())
---
# FunctionTests provide a solution for testing your logic and error handling.
# See the FunctionTest documentation for a full description of their
# capabilities.
apiVersion: koreo.dev/v1beta1
kind: FunctionTest
metadata:
  name: simple-example.v1
  namespace: koreo-demo
spec:

  # Specify the Function to test.
  functionRef:
    kind: ValueFunction
    name: simple-example.v1

  # Provide a base set of inputs.
  inputs:
    enabled: true
    values:
      int: 4

  # Define your test cases, each list-item is a test case.
  testCases:
  # Test the happy-path return.
  - expectReturn:
      allowedRange:
        lower: 2
        upper: 8
      lowerWords: [north, south, east, west]

  # Tweak the input, test again. This input tweak will carry forward.
  - inputOverrides:
      values:
        int: 16
    expectReturn:
      allowedRange:
        lower: 8
        upper: 32
      lowerWords: [north, south, east, west]

  # Tweak the input and test an error case. Due to `variant`, this will not
  # carry forward.
  - variant: true
    inputOverrides:
      values:
        int: 0
    expectOutcome:
      permFail:
        message: must be positive

  # Tweak the input and test another other error case. Due to `variant`, this
  # will not carry forward.
  - variant: true
    inputOverrides:
      enabled: false
    expectOutcome:
      skip:
        message: User disabled
```
