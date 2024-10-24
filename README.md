**Homework_5_1**统计数据集上市公司股票代码（“stock”列）的出现次数，按出现次数从⼤到⼩输出，输出格式为：<排名>,<股票代码>,<次数>
在本地VsCode进行编程，通过mvn clean package进行打包，在虚拟机中配置好的Hadoop环境下执行：hadoop jar ~/Desktop/Homework_5_1-1.0-SNAPSHOT.jar /local/analyst_ratings.csv /output/Homework_5_1_temp /output/Homework_5_1_final
结果位于Output的Homework_5_1中，其中temp存储的为未经排序的结果，final为最终结果

**Homework_5_2**统计数据集热点新闻标题（“headline”列）中出现的前100个⾼频单词，按出现次数从⼤到⼩ 输出。要求忽略⼤⼩写，忽略标点符号，忽略停词（stop-word-list.txt）。输出格式为：<排名>,<单词>,<次数>。
执行命令：hadoop jar ~/Desktop/Homework_5_2-1.0-SNAPSHOT.jar /local/analyst_ratings.csv /local/stop-word-list.txt  /output/Homework_5_2_temp /output/Homework_5_2_final
结果位于Output的Homework_5_1中，其中temp存储的为未经排序的结果，final为最终结果
