# Contributing to BGPMon
## Coding standards
We use the [Code Review Comments](https://github.com/golang/go/wiki/CodeReviewComments) as the basic standard for all of our code.
In addition to this, we have supplemental standards defined below.

### Gofmt and golint
All code should be formatted with gofmt and pass golint without any errors. This helps to avoid small formatting errors and lack
of documentation.

### Import Style
All imports within the project should be contained within an import() block, even if there's only one. Gofmt will sort imports
within a group alphabetically. Import groups are separated by a newline. There should only be 3 import groups, appearing in order.

1. Standard library
2. Local project packages
3. 3rd party libraries

Some files may not use all of the groups above. Here is an example of the import style:

```go
    import (
        "fmt"
        "os"

        "github.com/CSUNetSec/bgpmon/util"

        "github.com/spf13/cobra"
    )
```
    
### Variables
Multi-word variables should use camelCase. The first letter should not be capitalized, as that creates an exported name in golang.
For consistency, even variables within functions, that are not exported, should use the same style. This also applies to fields
within structs, and unexported function declarations. Exported names should use CamelCase, with the first letter capitalized.

```go
    func unexportedFunc() {}
    func ExportedFunc() {}
    
    struct example {
        exampleInt int
    }
```

### Declarations in if statements
In golang, it is possible to declare mulitple variable in the conditional line of an if statement. The scope of this variable 
is the extent of the if and any attached else clauses.

```go
    if a, b := foo; a != b {
        // a and b are in scope here
    } else {
        // Also in scope here
    }
    // Neither are in scope here
```

This type of statement is **not** allowed in BGPMon. Instead, the following style is preferred:

```go
    a, b := foo()
    if a != b {
    }
```

There is only one exception to this: error handling. An error can be declared and handled in the same line:

```go
    if err := foo(); err != nil {
        // Handle err
    }
```
    
### Comments
#### Documentation Comments
All comments that appear in godoc documentation must use be complete sentences with proper grammar and spelling. Some strings
within the code, such as cobra **Short** and **Long** fields are also considered documentation, and should follow the same
standards.
#### Struct and Function comments
All exported names, as well as complex unexported names should have a brief comment before their declaration explaining their
purpose. Simple names (1 line functions, small structs) do not need a comment.
#### Global Blocks
Global blocks such as var() and const() blocks should have a comment explaining the fields declared within them. If a block is
using the **iota** keyword, individual fields should not have a comment, but the whole block should still have one.
#### Comment style
Comments should use the // over the /**/ style unless they are very long. A space should be added between the start of the
comment and its contents.

    // Space


