# abcs

This is a dcf data source project, initialized with `dcf::dcf_add_source`.

You can us the `dcf` package to check the project:

```R
dcf_check_source("abcs", "..")
```

And process it:

```R
dcf_process("abcs", "..")
```


Denominator data is obtained from the ABCs surveillance matrix for pneumococcal disease, extracted with Claude to a csv file. [https://www.cdc.gov/abcs/downloads/abcs_pop_matrix.pdf](https://www.cdc.gov/abcs/downloads/abcs-surveillance-matrix.pdf)
