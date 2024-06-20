# 问题

1. 在已经根据key的第一个字段进行排序前提下，两个reducer的输出结果依然与一个reducer的不一样

   ```python
   'mapreduce.partition.keypartitioner.options': '-k1,1',
   ```

2. 





# 心得

1. 使用 -r hadoop 和 不使用产生的结果截然不同