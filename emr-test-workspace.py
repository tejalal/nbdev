#!/usr/bin/env python
# coding: utf-8

# ### Load CSV from S3

# In[7]:


s3_path = "s3://emr-eks-data-bucket/input-data/sample-data.csv"

df = spark.read.csv(s3_path, header=True, inferSchema=True)
df.show()


# ### Apply business logic

# In[8]:


df_subset = df.select("City", "State",  "LocationType")
df_subset.show()


# In[ ]:


### Save processed data to S3


# In[9]:


df_subset.write.mode("overwrite").option("header", True).csv("s3://emr-eks-data-bucket/output-data/output-data2.csv/")





