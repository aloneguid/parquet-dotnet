# Working with Floating Point Data

Parquet.Net supports the following types to represent floating point data:

## SchemaElement\<float\>

32-bit single-precision floating point type. Values range from -3.4 x 10<sup>38</sup> to +3.4 x 10<sup>38</sup>

## SchemaElement\<double\>

64-bit double-precision floating point type. Values range from (+/-)5.0 x 10<sup>-324</sup> to (+/-)1.7 x 10<sup>308</sup>

## SchemaElement\<decimal\>

128-bit precise decimal values with 28-29 significant digits. Value range: (-7.9 x 10<sup>28</sup> to 7.9 x 10<sup>28</sup>) / 10<sup>0 to 28</sup>

