# Workflow

`Workflows` specify which Functions should be run, how their outputs map into
inputs, and manages their execution. In essence, they define a "controller".
That is, `Workflow` is a control-loop driven workflow orchestrator.

`Workflow` definitions are simple. They specify the resource type that will
cause the `Workflow` to run, provide configuration values to the entry-point,
perform a set of steps, and optionally surface conditions or state.

Think of a `Workflow` as a specification which is _instantiated_ with
configuration. Once instantiated, an instance of the `Workflow` will run
according to its configuration. Many instances of a `Workflow` may exist and
run concurrently. Many `Workflows` may be defined within one system, and
`Workflows` maybe be composed.

A `Workflow` is responsible for running _Logic_, which is a `ValueFunction`,
`ResourceFunction`, or another `Workflow`. _Logic_ should be thought of as
defining the body of a loop. The `Workflow` schedules iterations of that loop,
and manages the "external" (to that Logic's body) state interactions.

## Specification Reference

| Full Specification            | Description           |
| :-----------------------------| :-------------------- |
| **`apiVersion`**: `koreo.dev/v1beta1` | Specification version |
| **`kind`**: `Workflow`        | Always `Workflow` |
| **`metadata`**:               | |
| **`  name`**:                 | Name of the `Workflow`|
| **`  namespace`**:            | Namespace |
| **`spec`**:                   | |
|  *`  crdRef`*:                | _Optional_ Used to trigger the workflow and provide configuration values. |
|  *`    apiGroup`*:            | |
|  *`    version`*:             | |
|  *`    kind`*:                | |
| **`  steps`**:                | A collection of Functions or `Workflows` that provide the Logic. |
| **`  - label`**:              | Name of the step, must be alphanumeric and may contain underscores. Other steps will use this value to reference this step's return value. |
|  *`    ref`*:                 | A reference to the Logic to be run. May not be specified with `refSwitch`. |
| **`      kind`**:             | `ValueFunction`, `ResourceFunction`, or `Workflow` |
| **`      name`**:             | Name of the Function to use. |
|  *`    refSwitch`*:           | Allows selection of the Logic to be run, but requires all to share an interface. Either `ref` or `refSwitch` must be provided. |
| **`      switchOn`**:         | An expression who's value determines the Logic to run. Has access to `steps` and `inputs` at evaluation time. Must be a string. |
| **`      cases`**:            | List of cases and Logic to run. At least 1 is required. |
| **`      - case`**:           | A string that the `switchOn` expression will be exactly matched against. |
|  *`        default`*:         | One case may be specified as the default if no other cases are an exact match. |
| **`        kind`**:           | `ValueFunction`, `ResourceFunction`, or `Workflow`. |
| **`        name`**:           | Name of the Function to use. |
|  *`    skipIf`*:              | _Optional_ Provide a test to determine if the step should be run. This may be a Koreo Expression which has access to `steps` at evaluation time. |
|  *`    forEach`*:             | Allows for "mapping" over a list of values. |
| **`      itemIn`**: **`=[]`** | This must be a Koreo Expression that evaluates to a list. Each item will be mapped to the `inputKey`, and the Logic will be invoked once for each item. |
| **`      inputKey`**:         | The input name the item should be provided as to the logic. |
|  *`    inputs:`*: **`{}`**    | _Optional_ If provided, must be an object that specifies input values to the Logic. Koreo Expressions may be used by starting the value with an `=`. |
|  *`    condition`*:           | _Optional_ The result of the Logic will be set as a `status.condition` on the trigger object.|
| **`      type`**:             | The condition's "key", must be PascalCase. |
| **`      name`**:             | A short, descriptive name used within condition messages. |
|  *`    state:`*: **`{}`**     | _Optional_ If provided, must be an object that specifies values to be set on the trigger object. The return value from the Logic is accessible within `value`. Subsequent steps _may_ replace the values. |

## Usage

The following sections elaborate on the key features of `Workflow` and their
intended uses.

### `spec.crdRef`: Running a Workflow

A `Workflow` may be _externally_ triggered to run, and have its _configuration_
provided by an object specified using `spec.crdRef`. This object serves to
provide the `Workflow`'s configuration and the `Workflow` instance may
optionally report its conditions and state within this object's `status` block.

We refer to the _instance_ of the `spec.crdRef` object which _triggers_ a
`Workflow` as its 'parent' or 'trigger'.


> 🚧 Warning
>
> Though it is possible, it is not advised to use a resource which is
> controlled by another controller. Instead create your own CRDs. Koreo
> Developer Tooling provides a tool to generate a CRD from a `Workflow`.

### `spec.steps`: Defining the Logic

Each "step" defines some Logic to be called, specifies the inputs the Logic is
to be provided with, specifies an optional status condition, and optionally
specifies any state you wish exposed within the parent object's `status.state`.

Each step must specify the Logic to be called. Logic is defined by
`ValueFunction`, `ResourceFunction`, or using a sub-`Workflow` to compose
Functions. To _reference_ the Logic, you specify the Kind and the name. Logic
may be statically specified using `ref`, which specifies exact Logic to run.
Logic may be dynamically selected from a fixed set of references using
`refSwitch`, which provides the ability to select between multiple Logics which
implement a compatible interface—this is discussed in more detail below.

Steps also specify `inputs` to be provided to the Logic. For Functions, the
inputs are directly accessible within `inputs`. For `Workflows`, the inputs are
exposed under `parent`. That enables a `Workflow` to be directly triggered via
a `crdRef` _or_ it may be directly called as a sub-workflow. That makes reuse
and testing of `Workflows` easier.

The `Workflow` may pass input values from prior steps using `steps`. Values
from the parent object may be passed using `parent`.

A step may also specify a `forEach` block, which will cause the Logic to be
executed once per item in the `forEach.itemIn` list. Each item will be provided
within `inputs` with the key name specified in `forEach.inputKey`. This makes
using any Function within a `forEach` viable.

Steps may be conditionally run using `skipIf`. When the `skipIf` evaluates to
true, the step and its dependencies are [Skipped](/docs/glossary.md#skip)
without resulting in an error by stopping further evaluation of the step and
its dependencies.

Logic may be dynamically selected from a set of choices using `refSwitch`.
`refSwitch` allows allows Logic to define a common interface, then for the
`Workflow` to call the appropriate Logic based on input or computed values. The
`switchOn` expression is has access to the return values from prior
steps within `steps`. It also has access to the `inputs` that will be provided
to the Logic. Using `inputs` enables `refSwitch` to work with `forEach` and
dispatch the correct Logic for each item.

A step may expose a Condition on the parent resource using `condition`. The
Condition's type will match `condition.type`, and this should be unique within
your `Workflow`. Note that uniqueness is intentionally not enforced so that you
may update / change conditions subsequently, but you should be cautious about
reusing the same type since it makes debugging much harder. `condition.name` is
used within the condition message sentence to make human-friendly status
messages. It should be a meaningful name or _short_ descriptive phrase.

> 🚧 Warning
>
> Be careful not to accidentally step on the `condition.type` value as it makes
> debugging much harder, and reduces visibility into a `Workflow`'s status.

The Logic's results may be exposed via the `state` key. The Koreo Expressions
within the `Workflow` step may access the Logic's return value within `value`.
If specified, `state` must be a mapping and it will be _merged_ with other
`step.sate` values. This allows for fine control over what and how state is
exposed.

> 🚧 Warning
>
> If multiple steps set the same state keys, the return values will be merged.
> This can lead to confusing values so be cautious.

## Example Workflow

The following `Workflow` demonstrates some of the capabilities.

```yaml
apiVersion: koreo.dev/v1beta1
kind: Workflow
metadata:
  name: simple-example.v1
  namespace: koreo-demo
spec:

  # Creation, modification, or deletion of a TriggerDummy will trigger
  # this Workflow to run. That is, this workflow will act as a _controller_ of
  # this resource type.
  crdRef:
    apiGroup: demo.koreo.dev
    version: v1beta1
    kind: TriggerDummy

  # Steps may be run once all steps they reference have been run. To help make
  # the sequencing clearer, you are required to list steps after any step(s)
  # they reference. Note that steps may run concurrently as their dependencies
  # complete so you should not depend on the order in which they are listed.
  steps:
    # Each step must have a label, this label is an identifier. The return
    # value of the Logic is available to be passed into other steps' inputs as
    # `steps.simple_return_value`. If a step does not return successfully, then
    # any step referencing it will automatically be skipped and marked as
    # `depSkip`.
    # The 'parent' object's `metadata` and `spec`, will be passed to this
    # ValueFunction as `inputs.parent`, and the return value of this Function
    # will be available to other steps as `steps.config`.
    - label: config
      ref:
        kind: ValueFunction
        name: simple-example-config.v1
      inputs:
        string: =parent.spec.string
        int: =parent.spec.int

    - label: simple_return_value

      # The Logic to be run.
      ref:
        kind: ValueFunction
        name: simple-return-value.v1

      # The inputs are available within the Logic under `=inputs.`
      # `step.inputs` must be a mapping, but these may be Koreo Expressions,
      # simple values, lists, or objects. Use an expression to pass the return
      # value from another step.
      inputs:
        string: =steps.config.string
        int: =steps.config.int

      # Some or all of the return value may be surfaced into the parent's
      # `status.state`. This is useful to cache values or to surface them to
      # other tools such as a UI or CLI. By default, nothing is surfaced.
      state:
        config:
          nested_string: =value.nested.a_string
          empty_list: =value.empties.emptyList

    - label: resource_reader
      ref:
        kind: ResourceFunction
        name: resource-reader.v1

      # `step.inputs` may be a more complex structure, and Koreo Expressions
      # may be used for specific subkeys or nested within an object or list.
      inputs:
        name: resource-function-test
        validators:
          skip: false
          depSkip: false
          permFail: false
          retry: false
          ok: false
        values:
          string: =steps.config.string
          int: =steps.config.int

    - label: resource_factory
      ref:
        kind: ResourceFunction
        name: resource-factory.v1

      # Steps may be run once per item in an array. Each may be run
      # concurrently, so there is no execution ordering guarantee. The return
      # value of this step is an array of the return values who's order matches
      # source array's order. Each iterated value is provided to the Logic as
      # `=inputs[forEach.inputKey]`. That is, the value of input key is the
      # subkey withihn inputs.
      forEach:
        itemIn: =["a", "b", "c"]
        inputKey: suffix

      inputs:
        name: resource-function-test
        validators:
          skip: false
          depSkip: false
          permFail: false
```
