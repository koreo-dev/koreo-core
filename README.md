# koreo
Koreo provides three core primitives: Configuration, Functions, and Workflows.

_Functions_ are a means of templatizing KRM resources. They are "pure" in the
functional programming sense; they are side effect free. The _Function_ type
allows you to specify the needed inputs, validations, computations, outcome
evaluation, and results extraction. Most commonly they are used to construct
KRM resources, but this isn't required. In some cases they are used to
restructure data or evaluate other outcomes. They are well-typed, testable,
and deterministic.

_Workflows_ provide a mechanism to perform complex actions, such as running
one or more _Functions_ via a D.A.G. and evaluating the overall results.




