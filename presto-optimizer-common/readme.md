# Presto optimizer common

Set of optimizer utilities which are common to presto-main and other connectors in presto. Currently, connectors cannot directly
depend on presto-main or presto-main-base. As a result, a lot of code duplication can result because each connector has
their own set of optmizer plan rewriters which can utilize the utilities already available with presto-main module.

One such utility is ExpressionEquivalence i.e. establish if two row expressions are equivalent. Since this module
is shared across connectors it's public api needs to be stable.
