# Koreo Glossary of Terminology

## C

### Condition
A convention used on Kubernetes resources to communicate status information.
Koreo may optionally set Conditions on a [parent object](#parent-object) based
on the [Outcome](#outcome) from a [step](#step).

### Contract Testing
Used to ensure that correctly structured API calls are made based on a set of
inputs.

### Control Loop
A control loop observes conditions and state. If the observed conditions or
state do not meet the target state, then the control loop will attempt to bring
them into alignment with the target state by making adjustments.

## D

### `DepSkip`
An [Outcome](#outcome) that indicates a dependency is not yet ready. It means
the [Logic](#logic) was skipped without an attempt to evaluate.

## E

### Expression
See [Koreo Expression](#koreo-expression)

## F

### Function
Refers to a [`ValueFunction`](#valuefunction) or
[`ResourceFunction`](#resourcefunction).

### Function Test
Koreo's built in [control loop](#control-loop) friendly testing framework.
Allows for unit-testing style validation in addition to [contract
testing](#contract-testing).

### Function Under Test
Refers to a [`ValueFunction`](#valuefunction) or
[`ResourceFunction`](#resourcefunction) that is being tested by a
[`FunctionTest`](#functiontest).

## K

### Koreo
Arguably the most pleasant to use Kubernetes templating and workflow
orchestration system currently in existence.

### Koreo Expression
A simple expression language that is modelled after
[CEL](https://github.com/google/cel-spec/blob/master/doc/langdef.md), provides
capabilities needed for basic logic, arithmetic, string manipulation, and data
reshaping.

## L

### Logic
Refers to a [Function](#function) or [`Workflow`](#workflow). Most often the
term is used to refer to the [Function](#function) or [`Workflow`](#workflow)
to be run as a [`Workflow`](#workflow) [step](#Step).

## M

### Managed Resource
A Kubernetes resource that a [`ResourceFunction`](#resourcefunction) is
managing to ensures its specification matches a [Target Resource
Specification](#target-resource-specification) or reads values from (for
`readonly` functions).

## O

### `Ok`
The [Outcome](#outcome) that indicates a successful evaluation. A return-value
may be present, if expected.

### Outcome
Refers to the _return type_ of a [Function](#function) or
[`Workflow`](#workflow), the types are [`Skip`](#skip), [`DepSkip`](#depskip),
[`Retry`](#retry), [`PermFail`](#permfail), and [`Ok`](#ok).

## P

### Parent Object
A Kubernetes object which is used to trigger [`Workflow`](#workflow)
[reconciliations](#reconcile) and provide configuration to the
[`Workflow`](#workflow) instance.

### `PermFail`
An [Outcome](#outcome) that indicates a permanent failure condition that will
require intervention in order to resolve.

## R

### Reconcile
To run a [control loop](#control-loop) in order to ensure the conditions and
observed state match the desired state. If they do not match, the differences
will be _reconciled_ to bring them into alignment.

### `ResourceFunction`
A [Function](#function) which manages or reads values from a [Managed
Resource](#managed-resource). These functions are an interface to external
state, which they may set and load. When managing a resource, they define a
[control loop](#control-loop).

### `ResourceTemplate`
Provides a simple means of specifying static values as a base [Target Resource
Specification](#target-resource-specification). A `ResourceTemplate` may be
dynamically loaded by a [`ResourceFunction`](#resourcefunction), allowing for
configuration based template selection. The static values may be overlaid with
values provided to (or computed by) a [`ResourceFunction`](#resourcefunction).

### `Retry`
An [Outcome](#outcome) that indicates the [Logic](#logic) should be retried
after a specified delay. Typically this indicates an active waiting status that
is expected to self-resolve over time.

## S

### `Skip`
An [Outcome](#outcome) that indicates the [Logic](#logic) was skipped without
an attempt to evaluate due to an input or other condition.

### State
Some of all of a [Logic's](#logic) return value which will be set on the
[parent object's](#parent-object) `status.state` property.


### Step
A [`Workflow`](#workflow) step specifies [Logic](#logic) to be run, how inputs
from other steps will map into the [Logic's](#logic) inputs, if a
[Condition](#condition) should be reported, and if any [`state`](#state) should
be extracted and returned (either to a calling [`Workflow`](#workflow) or
parent object).

## T

### Target Resource Specification
The specification that a resource is expected to match after all [Koreo
Expressions](#koreo-expression) have been evaluated and all overlays applied.
The is the fully materialized resource view that will be applied to the
cluster.

## V

### `ValueFunction`
A pure function which may be used to perform validations, compute values, or
restructure data.

## W

### `Workflow`
Defines a collection of [Steps](#Step) to be run and manages their execution.
