## Frequent term triad mining in news titles (16 marks)

**Background:** In this problem, we utilize a data set containing news categories and titles. Your task is to find out the top-k most frequent term triads in each category. The occurrence of a term triad (u, v, w) is defined as: u, v and w appear in the same article headline (i.e., (v, w, u) (u, w, v) and (u, v, w) are treated the same).

**Problem Definition:** The dataset you are going to use contains data of news headlines published in different categories. In this text file, each line is a headline of a news article, in the format of "**category,headline**". The headline texts and category are separated by a comma (see the example dataset).

**Output Format:** You need to ignore the stop words such as “to”, “the”, “in”, etc. (refer to the broadcast variable on how to do this efficiently). A stopword list is stored in *stopwords.txt .*

Please get the terms from the dataset as below:

- Use the following split function to split the documents into terms:

import re re.split("[\\s*$&#/\"'\\,.:;?!\\[\\](){}<>~\\-_]+", line)

- Ignore the letter case, i.e., consider all words as lowercase.
- Ignore terms that contain non-alphabetical characters, you can use `isalpha().`
- Ignore the stop words such as "to", "the", "in", etc.

The Relative Frequency value of a term triad (u, v, w) is defined as:

𝑅𝐹𝑐𝑎𝑡(𝑢,𝑣,𝑤)=𝑁𝑐𝑎𝑡(𝑢,𝑣,𝑤)/𝑁𝑐𝑎𝑡

where Ncat(u, v, w) is the absolute count of term triad (u, v, w) occurs together in category "cat" and Ncat is the number of **valid** headlines in this category. 

Your Spark program should generate a list of (**k** \* total number of categories*) results, each of which is in the format of “Category**\t**Term1**,**Term2,Term3**:**relative frequency value” (the three terms are sorted in alphabetical order and separated by “***,\***”). The results should be first ranked by the category in alphabetical order, then by the relative frequency of a term triad in descending order, and finally by the term triad in alphabetical order. 

The number of invalid headlines is also of interest. A headline is considered invalid if it is empty or contains fewer than 3 **valid terms**. A term is valid if it is not a stopword and all the characters are alphabet letters (a-zA-Z). So in the output, the first line of each category should be the number of invalid headlines in the format of "Category**\t**invalid line:number". 

Given *k* = 2 and the sample dataset, the output is like:

business    invalid line:2 business    asia,discouraging,news:0.6 business    asia,discouraging,stocks:0.6 medicine    invalid line:1 medicine    blood,contamination,hospital:0.6 medicine    alzheimer,blood,predict:0.4 technology    invalid line:2 technology    cutting,jobs,microsoft:0.5 technology    android,google,sdk:0.3333333333333333



Note that we also provide another testcase for you with *news.csv* and top2 result in ***newstop2output (updated at 8pm 27 June)*****.** 

**Code Format:** The code template has been provided. You need to submit two solutions, one using only RDD APIs and the other one using only DataFrame APIs. Your code should take three parameters: the input file, the output folder, the stopword file and the value of k. Assuming k=2, you need to use the command below to run your code:

$ spark-submit project2_rdd.py "file:///home/sample.csv" "file:///home/output" "file:///home/stopwords.txt" 2

## **Submission**

**Deadline: Monday 15th July 11:59:59 PM**

If you need an extension, please apply for a special consideration via “myUNSW” first. You can submit multiple times before the due date and we will only mark your final submission. To prove successful submission, please take a screenshot as the assignment submission instructions show and keep it to yourself. If you have any problems with submissions, please email [yi.xu10@student.unsw.edu.au](mailto:yi.xu10@student.unsw.edu.au). 

## **Late submission penalty**

5% reduction of your marks for up to 5 days, submissions delayed for over 5 days will be rejected.

## **Some notes**

1. You can read the files from either HDFS or the local file system. Using the local files is more convenient, but you need to use the prefix "file:///...". Spark uses HDFS by default if the path does not have a prefix.
2. You are not allowed to use numpy or pandas, since we aim to assess your understanding of the RDD/DataFrame APIs.
3. You can use coalesce(1) to merge the data into a single partition and then save the data to disk. You can save data to disk once in your program.
4. In the DataFrame solution, it is not allowed to use the spark.sql() function to pass the SQL statement to Spark directly.
5. It does not matter if you have a new line at the end of the output file or not. It will not affect the correctness of your solution.

## **Marking Criteria (**RDD solution 8.5 marks, DataFrame solution 7.5 marks**)**

- You must complete this assignment using Spark RDD/DataFrame APIs. Submissions only contain regular Python techniques will be marked as 0.
- Submission can be compiled and run on Spark => +3
- Submission can obtain correct top-k results (including correct item sets, correct RF values, correct format and order) => +3
- Submission can obtain correct invalid line (including correct number and output format) => +0.5
- Submissions correctly using Spark APIs (RDD/DataFrame solution only RDD/DataFrame APIs allowed) => +0.5
- Submissions with excellent code format and structure, readability, and documentation => 0.5
- **The efficiency of top-k computation (for RDD) => +1**