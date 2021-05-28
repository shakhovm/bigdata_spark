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

# Параметри

Master node - Standard (1 master, N workers)

Machine type - n1-standard-2

Number of GPUs - 0

Primary disk type - pd-standard

Primary disk size - 500GB

Local SSDs - 0

Worker nodes - 2

Machine type - n1-standard-2

Number of GPUs - 0

Primary disk type - pd-standard

Primary disk size - 500GB

Local SSDs - 0

RAM - 7.5GB

CPU Cores - 2

# Розмірі файлів

Розмір вхідних файлів - 500 мб (~60 мб кожний)

Розмір вихідних даних - 5 мб

# Час виконання кожного таску

## US

task 1 11.739160299301147

task 2 7.175024747848511

task 3 6.8612446784973145

task 4 4.929460525512695

task 5 4.645

task 6 4.402409791946411

## RU

task 1 7.236494302749634

task 2 6.522789716720581

task 3 13.717443704605103

task 4 6.502759695053101

task 5 5.54886794090271

task 6 5.162622690200806

## KR

task 1 5.014588356018066

task 2 5.182595729827881

task 3 6.826245307922363

task 4 4.255865812301636

task 5 3.989820957183838

task 6 3.6458683013916016

## JP
task 1 3.9974794387817383

task 2 3.9252591133117676

task 3 5.091667175292969

task 4 3.597850799560547

task 5 3.3164613246917725

task 6 3.2062628269195557

## IN

task 1 4.910714864730835

task 2 5.003216743469238

task 3 7.750786304473877

task 4 4.521589279174805

task 5 4.100409984588623

task 6 4.184681177139282

## GB


task 1 4.818311452865601

task 2 5.222165822982788

task 3 4.8473405838012695

task 4 4.066993474960327

task 5 3.8221898078918457

task 6 3.8211262226104736
