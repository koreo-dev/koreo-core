# ResourceTemplate

In order to make customizations easier, Koreo provides `ResourceTemplate`.
`ResourceTemplate` allows static resources to be defined which
`ResourceFunctions` may then _overlay_ with dynamic values to produce a fully
materialized resource. `ResourceFunctions` can dynamically compute the
`ResourceTemplate` name, making it easy to support a range of use cases and
configurations for a managed resource. By allowing the statically defined
resource to be dynamically loaded, it reduces the need to create complex or
verbose functions.

For instance, resource templates may be provided for different environments,
for specific resource types, or dynamically supplied configuration values.
Templates are also useful for simple static templates to provide common
configuration, such as regions. This allows the `ResourceFunction` to be
responsible for defining the interface and applying the values, but templates
to supply the bulk of "static" configuration.

This model makes it easy to turn existing resources into templates, then use a
function only to apply dynamic values.


| Full Specification         | Description           |
| :--------------------------| :-------------------- |
| **`apiVersion`**: `koreo.dev/v1beta1` | Specification version |
| **`kind`**: `ResourceTemplate` | Always `ResourceTemplate` |
| **`metadata`**:            | |
| **`  name`**:              | Name of the `ResourceTemplate`|
| **`  namespace`**:         | Namespace |
| **`spec`**:                | |
| **`  template`**:          | The Function Under Test. |

## Usage

The following sections elaborate on the key features of `ResourceTemplate` and
their intended uses.

### `spec.template`: Static Resource Specification

The `spec.template` is a static Target Resource Specification. Both
`apiVersion` and `kind` must be provided, but everything else is optional. This
static template will be (optionally) overlaid within the `ResourceFunction`.
The `metadata.name` and `metadata.namespace` properties are _always_ overlaid
by the `ResourceFunction`, so you need not specify them.


## Example ResourceTemplate

The following `ResourceFunction` demonstrates some of the capabilities.

```yaml
apiVersion: koreo.dev/v1beta1
kind: ResourceTemplate
metadata:
  # The template will be looked up by its name.
  name: docs-template-one.v1
  namespace: koreo-demo
spec:
  # Template contains the static resource that will be used as the base.
  # apiVersion and kind are required. The template is the actual body, or some
  # portion thereof, which you'd like to set static values for.
  template:
    apiVersion: koreo.dev/v1beta1
    kind: TestDummy
    metadata:
      labels:
        docs.koreo.dev/example: template-label
    spec:
      value: one
      nested:
      - 1
      - 2
---
apiVersion: koreo.dev/v1beta1
kind: ResourceTemplate
metadata:
  name: docs-template-two.v1
  namespace: koreo-demo
spec:
  template:
    apiVersion: koreo.dev/v1beta1
    kind: TestDummy
    metadata:
      labels:
        docs.koreo.dev/example: template-label
      annotations:
        docs.koreo.dev/example: template-two
    spec:
      value: two
      structure:
      - name: doc
      - name: examples
---
apiVersion: koreo.dev/v1beta1
kind: ResourceFunction
metadata:
  name: docs-template-function.v1
  namespace: koreo-demo
spec:
  locals:
    template_name: ="docs-template-" + inputs.template + ".v1"

  # The apiConfig section remains the same. For security purposes, apiVersion,
  # kind, name, and namespace will be overlaid onto the template.
  apiConfig:
    apiVersion: koreo.dev/v1beta1
    kind: TestDummy
    plural: testdummies

    name: =inputs.metadata.name + "-template-docs"
    namespace: =inputs.metadata.namespace

  resourceTemplateRef:
    name: =locals.template_name

    overlay:
      metadata: =template.metadata.overlay(inputs.metadata)
      spec:
        value: =inputs.value
        addedProperty: =inputs.value * 17
---
apiVersion: koreo.dev/v1beta1
kind: FunctionTest
metadata:
  name: docs-template-function.v1
  namespace: koreo-demo
spec:
  functionRef:
    kind: ResourceFunction
    name: docs-template-function.v1

  # Template 'one' will be the base case.
  inputs:
    template: one
    value: 42
    metadata:
      name: test-demo
      namespace: tests
      labels:
        docs.koreo.dev/from-function: label

  testCases:
  - label: Template One
    expectResource:
      apiVersion: koreo.dev/v1beta1
      kind: TestDummy
      metadata:
        name: test-demo-template-docs
        namespace: tests
        labels:
          docs.koreo.dev/example: template-label
          docs.koreo.dev/from-function: label
      spec:
        value: 42
        addedProperty: 714
        nested:
        - 1
        - 2

  - label: Template Two
    inputOverrides:
      template: two
    expectResource:
      apiVersion: koreo.dev/v1beta1
      kind: TestDummy
      metadata:
        name: test-demo-template-docs
        namespace: tests
        labels:
          docs.koreo.dev/example: template-label
          docs.koreo.dev/from-function: label
        annotations:
          docs.koreo.dev/example: template-two
      spec:
        value: 42
        addedProperty: 714
        structure:
        - name: doc
        - name: examples
```
