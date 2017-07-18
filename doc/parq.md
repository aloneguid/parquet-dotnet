# Parq

This tools gives a simple data inspector which lists out the columns found in a Parquet data set and the data values for those columns. 

To use, run ```dotnet parq.dll Mode=interactive InputFilePath=path/to/file.parquet DisplayMinWidth=10``` 

Arguments include:
* Mode (defaults to Interactive)
  * Interactive - breaks the dataset row-wise into folds and column-wise into sheets. Use the keyboard arrows to navigate the dataset
  * Full - displays all rows and columns of the dataset with no pause for user input. Limits the cell contents to the width of the header
  * Schema - displays no data, but lists all columns with their .NET type equivalent. Useful for inspecting content shape, for example when trying to build out model classes. 
* InputFilePath - path to the input parquet file
* DisplayMinWidth - as noted earlier, the interactive and full mode both display column contents up to the width of the header as we don't currently enumerate cell contents before beginning to draw. Setting DisplayMinWidth allows for long cell contents to be display.

![Parq](img/parq.png)