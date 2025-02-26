# ResourceFunction

A `ResourceFunction` interfaces with one resource in order to _manage_ it or to
_read values_ from it. The default behavior is to manage the resource. A
_manager_ `ResourceFunction`'s task is to ensure the resource's configuration
matches the target specification. _Read only_ `ResourceFunctions` are used to
wait for a resource to exist, wait for it to match some conditions, or to
extract values from the resource.

> 📘 Note
>
> The default behavior is to act as a _manager_ of the resource. All of the
> _mutation_ parameters (create, update, delete) discussed below only apply to
> _manager_ `ResourceFunctions`.

_Manager_ `ResourceFunctions` define a
[controller](https://kubernetes.io/docs/concepts/architecture/controller/)—if
the resource's configuration does not match expectations, it will take actions
to bring it into alignment with the target specification. There are several
configuration options which allow the developer to control how
`ResourceFunction` will manage its resource.

`ResourceFunction` provides the same capabilities and interface as
`ValueFunction` so that preconditions may be check and a return value computed.


| Full Specification            | Description           |
| :-----------------------------| :-------------------- |
| **`apiVersion`**: `koreo.dev/v1beta1` | Specification version |
| **`kind`**: `ResourceFunction` | Always `ResourceFunction` |
| **`metadata`**:               | |
| **`  name`**:                 | Name of the `ResourceFunction`|
| **`  namespace`**:            | Namespace |
| **`spec`**:                   | |
|  *`  preconditions`*:         | A set of assertions to determine if this function can and should be run, and if not the function's return-type. These are run in order and the first failure is returned. Exactly one return type is required. |
| **`    assert`**:             | Must be a Koreo Expression specifying an assertion, if the assertion evaluates false, the function evaluates to the specified result. |
|  *`    defaultReturn`*:       | Indicate that no further conditions should be checked, return `Ok` with the static value specified. |
| **`      {}`**                | The return value must be an object, but may be the empty object. |
|  *`    skip`*:                | Return a `Skip`. This is useful for functions which optionally run based on input values. |
| **`      message`**:          | The message to be returned is used for condition / status reporting. Make it descriptive of the reason for the return value. |
|  *`    depSkip`*:             | Return a `DepSkip`, indicating that a dependency is not ready. |
| **`      message`**:          | The message to be returned is used for condition / status reporting. Make it descriptive of the reason for the return value. |
|  *`    retry`*:               | Return a `Retry` which will cause the `Workflow` to re-reconcile after `delay` seconds. |
| **`      message`**:          | The message to be returned is used for condition / status reporting. Make it descriptive of the reason for the return value. |
| **`      delay`**:            | Seconds to wait before re-reconciliation is attempted. |
|  *`    permFail`*:            | Return a `PermFail` which will cause the `Workflow` not to re-reconcile until the parent has been updated. This is for fatal, unrecoverable errors that will require human intervention. |
| **`      message`**:          | The message to be returned is used for condition / status reporting. Make it descriptive of the reason for the return value. |
|  *`  locals`*:                | Locals allows for defining constant values or for naming interim expressions. Locals are evaluated _after_ `preconditions` but prior to `apiConfig`. Must be an object and the values may be accessed using `locals` in other Koreo Expressions. |
| **`  apiConfig`**:            | Defines the resource to be managed, the `apiVersion`, `kind`, `name`, and `namespace` are always applied over the Target Resource Specification for security reasons. |
| **`    apiVersion`**:         | The `apiVersion` of the managed resource. |
| **`    kind`**:               | The `kind` of the managed resource. |
|  *`    plural`*:              | *Required if* the _plural_ form of the `apiConfig.kind` is not a simple pluralization (lower-case `kind` + 's'). |
|  *`    namespaced`*:          | If the resource is _namespaced_, the default is `true`. This adjusts the API interactions and is useful for _Cluster Scoped_ resources. |
|  *`    owned`*:               | Add the 'parent' to the managed resource's owner reference list if _the namespace matches and `readonly=false`_. The default is `true`. |
|  *`    readonly`*:            | Indicate that the resource should not be managed. This will cause the function to ignore `create`, `update`, and `delete` configuration. Default is `false`. |
| **`    name`**:               | Specify the `metadata.name` field of the managed resource. This may be a Koreo Expression, when evaluated it has access to `inputs` and `locals`. |
| **`    namespace`**:          | Specify the `metadata.namespace` field of the managed resource. This may be a Koreo Expression, when evaluated it has access to `inputs` and `locals`. |
|  *`  resourceTemplateRef`*:   | The Target Resource Specification provided via a dynamically loaded template. May not be specified if `spec.resource` is provided. |
| **`    name`**:               | The name of a `ResourceTemplate` to source a static `template` from. May be a static string or a string-valued Koreo Expression with access to `inputs` and `locals` when evaluated. |
|  *`  resource`*:              | The Target Resource Specification that defines the resource to manage. This may be a Koreo Expression with access to `inputs` and `locals`. May not be specified if `spec.resourceTemplateRef` is provided. |
|  *`  overlays`*:              | An optional list of overlays to be sequentially applied over the Target Resource Specification. Each must be either an inline `overlay` or an `overlayRef`. |
|  *`  - overlay`*: **`{}`**    | An inline overlay specificiation. This may be a Koreo Expression and has access to `inputs`, `locals`, and the base Target Resource Specification as `resource`. |
|  *`    overlayRef`*:          | Use a `ValueFunction` as the overlay. The `ValueFunction`'s return value acts as the overlay, but all `ValueFunction` capabilities may be used. The current Target Resource Specification is exposed as `inputs.resource` within the `ValueFunction`. |
| **`      name`**:             | Name of the `ValueFunction` to use as the overlay. |
|  *`    inputs:`*: **`{}`**    | _Optional_ May only be provided for `overlayRef` overlays and specifies inputs to be provided to the overlay `ValueFunction`. Must be an object that specifies input values to the Logic. Koreo Expressions may be used and have access to `inputs` and `locals` at evaluation time. |
|  *`    skipIf`*:              | _Optional_ Provide a test to determine if the overlay should be applied. This must be a Koreo Expression which has access to `inputs` and `locals` at evaluation time. |
|  *`  create`*:                | Specifies if the managed resource should be created if it does not exist. The default is `enabled=true` with a 30 second `delay`. |
|  *`    enabled`*:             | Create the resource if it does not exist. Default is `true`. |
|  *`    delay`*:               | The number of second to wait after creating before re-reconciliation is attempted. Default is 30 seconds. |
|  *`    overlay`*:             | An overlay which is applied over the Target Resource Specification. This is useful for setting create-time-only values, such as immutable properties or external identifiers. This may be a Koreo Expression and has access to `inputs`, `locals`, and the current Target Resource Specification as `resource`. |
|  *`  update`*:                | The behavior when differences are detected, one of `patch`, `recreate`, or `never` must be specified. The default is `patch` with a 30 second `delay`. |
|  *`    patch`*:               | If there are differences in any fields defined in Target Resource Specification, patch to correct those differences. |
|  *`      delay`*:             | The number of seconds to wait to before a re-reconciliation to verify conditions after patching. Defaults to 30 seconds. |
|  *`    recreate`*:            | Delete and recreate the resource if any properties defined in the Target Resource Specification have differences. This is useful for immutable resources. |
|  *`      delay`*:             | The number of seconds to wait after deleting before attempting to recreate. Defaults to 30 seconds. |
|  *`    never`*: **`{}`**      | Ignore any differences—this is useful when a resource needs to exist, but its condition does not matter. |
|  *`  delete`*:                | Specify the deletion behavior. One of `abandon` or `destroy` must be specified. |
|  *`    abandon`*: **`{}`**    | Indicates that the resource should be left unmanaged. This is useful for stateful resources, such as storage, which can't be deleted without loss. |
|  *`    destroy`*: **`{}`**    | Destroy the resource if it becomes unmanaged. This is appropriate for stateless resources or those which are easily recreated. |
|  *`  postconditions`*:        | A set of assertions run after CRUD operations have run. These are run in order and the first failure is returned. Exactly one return type is required. |
| **`    assert`**:             | Must be a Koreo Expression specifying an assertion, if the assertion is false the function evaluates to the specified result. The expressions has access to the resource through `resource`, in addition to `inputs` and `locals`. |
|  *`    defaultReturn`*:       | Indicate that no further conditions should be checked, return `Ok` with the static value specified. |
| **`      {}`**                | The return value must be an object, but may be the empty object. |
|  *`    skip`*:                | Return with `Skip`. |
| **`      message`**:          | The message to be returned is used for condition / status reporting. Make it descriptive of the reason for the return value. |
|  *`    depSkip`*:             | Return with `DepSkip`, indicating that a dependency is not ready. |
| **`      message`**:          | The message to be returned is used for condition / status reporting. Make it descriptive of the reason for the return value. |
|  *`    retry`*:               | Return a `Retry` which will cause the `Workflow` to re-reconcile after `delay` seconds. |
| **`      message`**:          | The message to be returned is used for condition / status reporting. Make it descriptive of the reason for the return value. |
| **`      delay`**:            | Seconds to wait before re-reconciliation is attempted. |
|  *`    permFail`*:            | Return a `PermFail` which will cause the `Workflow` not to re-reconcile until the parent has been updated. This is for fatal, unrecoverable errors that will require human intervention. |
| **`      message`**:          | The message to be returned is used for condition / status reporting. Make it descriptive of the reason for the return value. |
|  *`  return`*:                | The return value for the function. Koreo Expressions may be used and may access `inputs`, `locals`, and the managed resource using `resource`. It must be an object. |


## Usage

The following sections elaborate on the key features of `ResourceFunction` and
their intended uses.

The sections are discussed in the order in which they are evaluated.

### `spec.preconditions`: Performing Validation

Preconditions are used in order to determine if it is possible to evaluate a
function or if it should be evaluated. For instance, configuration may allow
some functionality to be enabled or disabled by the user or an input value
might need checked to assert that it is within an allowed range.
`spec.preconditions` allows conditions to be _asserted_, and if they are not
met specifies an outcome to be returned.

### `spec.locals`: Static and Interim Values

`spec.locals` is useful for defining constant values (primitive types, lists,
or objects). Locals also allows expressions to be named, which can improve
readability of the return value expression and help with maintenance of a
Function.

> 📘 Note
>
> Currently, `spec.locals` may not reference other `locals`.

### `spec.apiConfig`: Managed Resource Configuration

`ResourceFunctions` are meant to manage an external resource, that resource is
defined by `spec.apiConfig`. In order to prevent dangerous escapes,
`apiVersion` and `kind` are static strings and must always be specified. These
are always overlaid onto the materialized resource view before it is applied to
the cluster.

`name` and `namespace` are also required to be defined, but they maybe be Koreo
Expressions with access to `inputs` and `locals` at evaluation time. Similar to
`apiVersion` and `kind`, these values are always overlaid onto the materialized
resource view before it is applied to the cluster. This prevents accidental
resource definitions or overlays that might inadvertently change the desired
name/namespace.

`plural` is required only for resources who's plural form is not a simple
pluralization. This is due to a highly unfortunate design decision of the
Kubernetes API Server which makes using the singular form harder. At some
point, a lookup mechanism will be implemented and this requirement will likely
be removed.

`owned` indicates if you would like the parent to be automatically added to the
managed resource's `metadata.ownerReferences` list. The reference will only be
added if the object is namespaced, within the same namespace as the parent, and
readonly is `false`.

`readonly` indicates that the resource is not being managed. This is useful
when a resource needs checked for existence or values extracted from a resource
which is managed by the user or another controller.


### `spec.resource`: Inline Target Resource Specification

For cases where only one "static" configuration is desired, _inline_ Target
Resource Specification may be used. It allows the function author to inline
Koreo Expressions into the resource body, removing the need for an additional
overlay step. This can make creating a managed resource feel similar to other
template solutions, but with the benefit that string manipulation directives
are not required to correctly structure the resource.

The Koreo Expressions used within the Target Resource Specification have access
to `inputs` and `locals`.

The `apiVersion`, `kind`, `metadata.name`, and `metadata.namespace` are always
computed and overlaid on top of the Target Resource Specification, so these may
be omitted.

### `spec.resourceTemplateRef`: Dynamically Loaded Target Resource Specification

When there are multiple "static" configurations of a resource, but there is a
desire to expose a common interface or configuration options, using _dynamic_
Target Resource Specification saves repetition by allowing the static component
to be dynamically loaded, and then overlays (which may contain Koreo
Expressions) to be applied.

The template `name` is a Koreo Expression, with access to `inputs` and `locals`
at evaluation time. This allows templates to be loaded dynamically. Conventions
should be used to make the names clear and consistent, for instance: `name:
="deployment-service-account-" + locals.templateName` to indicate that the
template is for a deployment's service account.

The `apiVersion`, `kind`, `metadata.name`, and `metadata.namespace` are always
computed and overlaid onto the Target Resource Specification, so these may be
omitted.

### `spec.overlays`: Atomic Overlays to Encapsulate Logic

`overlays` provides a mechanism to apply overlays as atomic units onto the
Target Resource Specification. Each overlay may be either inline (`overlay`) or
dynamic (`overlayRef`) and may be conditionally skipped (`skipIf`). This allows
full Target Resource Specifications to be gradually built by composing layers
that encapsulate intention and logic into testable units.

`overlays` may be used with both inline `resource` definitions or combined with
static `ResourceTempates` using `resourceTemplateRef`. When combined with
`ResourceTemplate` it creates a very flexible, but simple, mechanism for
swapping out static (and often verbose) base configurations and then
customizing them for a given use case. The `ResourceFunction`'s `preconditions`
and `locals` make it possible to ensure only allowed values are applied via
overlays, and only when appropriate.

Inline Overlay Koreo Expressions have access to `inputs`, `locals`, and the
current Target Resource Specification as `resource` so that static values are
available if needed.

Dynamic Overlays may be provided using `ValueFunctions`. This allows for the
use of all `ValueFunction` capabilities, such as `preconditions` and `locals`.
The `return` value defines the overlays to be applied. Koreo Expressions within
the `ValueFunction` have access to `inputs`, `locals`, and the current Target
Resource Specification as `resource` so that static values are available if
needed.

The `apiVersion`, `kind`, `metadata.name`, and `metadata.namespace` are always
computed and overlaid onto the Target Resource Specification, so these may be
omitted.

### `spec.create`: Customizing Creation

Creation may be turned on and off using `enabled`. If creation is not enabled,
and the managed resource does not exist, then the function will cause the
`Workflow` to wait for the resource to exist.

The `delay` controls how much time the `Workflow` should wait after creation
for the resource to be ready. For resources that reach ready-state instantly a
low delay value makes sense. For resources (such as a database), with longer
time-to-ready, there is little value in setting this number too low. Instead
set it close to (ideally slightly over) the typical expected time-to-ready.
This will minimize the number of unneeded calls to the API server.

Lastly, a custom `overlay` may be specified in order to set create-time-only
property values. Though infrequently needed, these are crucial for certain
applications such as interfacing with existing external resources or setting
immutable properties. `create.overlay` behaves similar to the other overlays in
that it is an object which may contain Koreo Expressions. The expressions have
access to `inputs`, `locals`, and `resource` at evaluation time. `resource` is
set to the current Target Resource Specification.

### `spec.update`: Flexible Update Handling

When resource differences are detected, there are three options to correct
them. There are also two directives which may be used to alter the difference
detection behavior for special cases.

The default behavior is to `patch` the differences in order to align them to
the Target Resource Specification. This is the most common, and the simplest
behavior. The Target Resource Specification is simply re-applied in order to
"correct" it. If there are any _immutable_ properties or properties which
should not be patched or monitored, use `create.overlay` to set those only at
create time. The `delay` specifies how long to wait after patching before
checking the managed resource's ready condition. The guidance for setting this
delay is similar to that for the create delay: set to median time-to-ready +
10% in order to reduce API server load.

For some resources, the best (or only) option is to delete and recreate when
differences are detected. For these, specify `update.recreate`. The resource
will be deleted, then after the specified `delay`, an attempt to create it will
be made. Set `delay` to the time it takes for the deletion and any finalizers
to run.

The final option is to simply ignore any differences, this is done using
`update.never`. In some cases this is the only option, in others the precise
resource specification does not matter—only that it exists.

Some resource _controllers_ may update properties within the spec. Typically
this is not an issue as the values should match what was provided. For
"arrays", however, this can be problematic. If the array is actually a _set_,
then its ordering may change. The same issue arises for mappings that are
flattened into an array with the key contained as a property within the list
objects. To handle these cases, Koreo provides two directives to configure the
difference detection logic for arrays:

    x-koreo-compare-as-set
    x-koreo-compare-as-map

These are embedded into the Target Resource Specification and will be stripped
prior to sending to the API. `x-koreo-compare-as-set` takes an array of
property names which should be treated as _sets_ rather than ordered arrays; it
may only be used on "simple" (boolean, numeric, and string) types.
`x-koreo-compare-as-map` takes a map of "arrays to treat as collections" and an
array of properties to use as the key within each mapping. See the examples
below for usage.


### `spec.delete`: Cleanup Behavior

As a `Workflow` definition changes, an instance configuration changes, or a
`Workflow` instance is deleted, managed resources may no longer be created. In
these cases, Koreo needs told how to handle the managed resource.

There are currently two options available: `abandon` or `destroy`. For
resources which contain data, `abandon` is recommended for production
environments. In the future, abandoned resources will be labeled to make them
easy to identify.

For stateless or fast-to-create resources, `destroy` will delete the managed
resource.

Note that in some cases these options are in addition to the capabilities of
the underlying managed resource's controller configuration. Be sure to
carefully review the controller's documentation to ensure the desired behavior.

### `spec.postconditions`: Performing Post-CRUD Validation

Postconditions are used to assert the managed resource is ready and meets some
set of conditions. The _assertion_ is a Koreo Expression which has access to
`inputs`, `locals`, and `resource` at evaluation time. `resource` contains the
actual object, allowing for inspection of values within `status`. This is
useful for examining the resource's `status.conditions`, for example, to ensure
the resource is ready before continuing. It is also useful when values need to
be extracted in order to pass them into other functions, such as with VPCs
where the subnets may only be runtime known.

The behaviors match `spec.preconditions`, with the addition of the `resource`
being available for use within the assertions. The object returned from the
Kubernetes API is contained within `resource`, allowing for assertions on any
values needed.

> 🚧 Warning
>
> You must ensure that any values used on `resource` are present. Use
> `has(...)` in order to assert the presence of a property.

### `spec.return`: Returned value

The return expression must be an object. It may use constant values, data
structures, or Koreo Expressions which have access to `inputs`, `locals`, and
`resource`. The object returned from the Kubernetes API is contained within
`resource`, allowing for processing of any values needed.

> 🚧 Warning
>
> You must ensure that any values used on `resource` are present. Use
> `has(...)` in order to assert the presence of a property.

## Example ResourceFunction

The following `ResourceFunction` demonstrates some of the capabilities.

```yaml
apiVersion: koreo.dev/v1beta1
kind: ResourceFunction
metadata:
  name: simple-resource-function.v1
  namespace: koreo-demo
spec:

  # Checking input values are within range or ensuring that a config is enabled
  # are common needs, preconditions support both use cases.
  preconditions:
  - assert: =inputs.values.int > 0
    permFail:
      message: |
        ="The int input value must be positive, received '"
         + string(inputs.values.int)
         + "'"

  - assert: =inputs.enabled
    skip:
      message: User disabled the ResourceFunction

  # Locals are especially useful for interim expressions to improve
  # readability, make complex expressions more ergonomic to write, or for
  # defining constant values for use within the return expression.
  locals:
    computedValues:
      halfed: =inputs.values.int / 2
      doubled: =inputs.values.int * 2

    constantList: [NORTH, SOUTH, EAST, WEST]

  apiConfig:
    apiVersion: koreo.dev/v1beta1
    kind: TestDummy
    plural: testdummies

    name: =inputs.metadata.name + "-docs"
    namespace: =inputs.metadata.namespace

  # An inline Target Resource Specification can be quite concise. This shows
  # how you can inherit common metadata, to ensure consistent labels for
  # example. This will also demonstrate the special compare directives (which
  # aren't commonly needed).
  resource:
    metadata: =inputs.metadata
    spec:
      directions: =locals.constantList
      range:
          top: =locals.computedValues.doubled
          bottom: =locals.computedValues.halfed

      # This is not often needed, but it is critical when it is required.
      x-koreo-compare-as-set: [aStaticSet]
      aStaticSet:
      - 1
      - 2
      - 3
    
      # This is not often needed, but it is critical when it is required.
      x-koreo-compare-as-map:
        collectionDemo: [name]
      collectionDemo:
      - name: first
        value: 1
      - name: second
        value: 2
      - name: third
        value: 3

  # The return value of a ResourceFunction must be an object, Koreo Expressions
  # have access to `inputs`, `locals`, and `resource`.
  return:
    ref: =resource.self_ref()
---
apiVersion: koreo.dev/v1beta1
kind: FunctionTest
metadata:
  name: simple-resource-function.v1
  namespace: koreo-demo
spec:
  functionRef:
    kind: ResourceFunction
    name: simple-resource-function.v1

  # Provde base, good inputs.
  inputs:
    metadata:
      name: test-demo
      namespace: tests
    enabled: true
    values:
      int: 64

  # Each testCase is an iteration of the control loop.
  testCases:
  # The first test creates the resource, and we verify it matches expections
  - label: Initial Create
    expectResource:
      apiVersion: koreo.dev/v1beta1
      kind: TestDummy
      metadata:
        name: test-demo-docs
        namespace: tests
      spec:
        directions: [NORTH, SOUTH, EAST, WEST]
        range:
          top: 128
          bottom: 32

        aStaticSet:
        - 1
        - 2
        - 3

        collectionDemo:
        - name: first
          value: 1
        - name: second
          value: 2
        - name: third
          value: 3

  # variant tests do not preserve anything into the next test-cycle. They're
  # useful for testing error or variant cases.
  - variant: true
    label: Set reordering is OK
    # This allows us to simulate an external resource mutation, such as a
    # controller or person.
    overlayResource:
      spec:
        aStaticSet:
        - 2
        - 1
        - 3

    # Check a return value, which indicates no changes are made
    expectReturn:
      ref:
        apiVersion: koreo.dev/v1beta1
        kind: TestDummy
        name: test-demo-docs
        namespace: tests

  - variant: true
    label: Collection reordering is OK
    overlayResource:
      spec:
        collectionDemo:
        - name: third
          value: 3
        - name: second
          value: 2
        - name: first
          value: 1

    # Or just check for an `ok` outcome, which indicates no changes were made.
    expectOutcome:
      ok: {}

  # We can also instruct the test matcher to treat a list as a set or map.
  # controller or person.
  - variant: true
    label: Test Comparision directives
    inputOverrides:
      values:
        int: 30
    expectResource:
      apiVersion: koreo.dev/v1beta1
      kind: TestDummy
      metadata:
        name: test-demo-docs
        namespace: tests
      spec:
        directions: [NORTH, SOUTH, EAST, WEST]
        range:
          top: 60
          bottom: 15

        # This instructs the _test_ validator to treat this as a set.
        x-koreo-compare-as-set: [aStaticSet]
        aStaticSet:
        - 3
        - 2
        - 1

        # This instructs the _test_ validator to treat this as a map.
        x-koreo-compare-as-map:
          collectionDemo: [name]
        collectionDemo:
        - name: second
          value: 2
        - name: third
          value: 3
        - name: first
          value: 1
```
