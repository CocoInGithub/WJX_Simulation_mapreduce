# WJX_Simulation_mapreduce
Python code implementation for simulating Mapreduce process

Mapreduce.py是过程的实现代码

代码涉及如下目录:  
1./HDFS目录：用于读取分页文件split.csv  
2./tmp/mapred/local目录：是模拟存放的过程中的缓存文件  

代码实现的过程主要有以下：  
1.将一个目标文件分成4个split.csv存放于HDFS目录下用于读取  
2.Mapfunc，生成相应的K和V  
3.模拟cache过程(用队列实现)，以及之后的Spill(简单的partition，sort)过程，merge存于磁盘(/tmp/mapred/local。  
4.再将Map后的file读入内存，进行merge file和目标的Reduce操作，找出飞行最多的乘客。  

                              
#              DataNodeManager      Spill(partition,sort...)    
#                ⬆                     ⬆
# split_i ————> Map ————> Cache ————> file_i ————> Mergefile ——Copy——> Memory ————> Merge ————> Reduce
