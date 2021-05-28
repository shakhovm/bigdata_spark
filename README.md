# bigdata_spark


Command to run application
--------------------------

```
$ python main.py <folder-with-csvs> <out-folder->
```
\<path-to-fil\> - path to folder with csv needed for task
\<out-folder\> - folder where to generate results
  

 
# Опис архітектури

Програма читає дані з google storage в форматі csv, передає воркерам на обробку, які використовують парадигму mapreduce для ефективної обробки, результати зберігаються також в google storage
# Репорти
Репорти знаходяться в архіві та на гугл стореджі за посиланням https://storage.googleapis.com/superbucker/all_results.zip
