# Databricks notebook source
# This is sample import statemenmts
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md ### Utility class

# COMMAND ----------

class TableUtilities(object):
  """
  Utilities used to fetch data
  """ 
  
  def _exclude_pattern(self,mylist,exclude_pattern_list):
    """
    method type: internal
    helper method to exclude patterns
    """
    item_for_removal = []
    for item in mylist:
      for pattern in exclude_pattern_list:
        if pattern in item:
          item_for_removal.append(item)
    mylist = list(set(mylist) - set(item_for_removal))
    return mylist
  
  def _getcommonletters(self,strlist):
    """
    method type: internal
    helper method for _findcommonstart
    """
    return ''.join([x[0] for x in zip(*strlist) \
                     if reduce(lambda a,b:(a == b) and a or None,x)])

  def _findcommonstart(self,strlist):
    """
    method type: internal
    helper method for table prefix
    """
    strlist = strlist[:]
    prev = None
    while True:
        common = self._getcommonletters(strlist)
        if common == prev:
            break
        strlist.append(common)
        prev = common

    return self._getcommonletters(strlist)

     
  def __getattr__(self, name):
    def method(*args):
      print("handling dynamic method " + name)
      if name in self.table_ref_map.keys():
        print "method exists"
        table_name = self.table_ref_map[name]
        return self.get_data_frame(table_name)
      else:
        return "method does not exist"
    return method

# COMMAND ----------

# MAGIC %md ### Base Data Object class

# COMMAND ----------

class BaseObject(TableUtilities):
  def __init__(self, domain = 'customer'):     
    TableUtilities.__init__(self)
    self.domain = domain

    self.env = "prd"
    self.layer = "published_standard"
    self.exclude_db_patterns = ["ichub", "cdl", "field", "bai"]
    self.exclude_table_patterns = ["cross_reference"]
    self.table_ref_map = self._table_ref_map()

  def _table_ref_map(self):
    """
    method type: internal
    helper method to initalize a table reference map
    """
    # fetch matching databases
    db_list = []
    df = spark.sql("show databases")
    for db in df.select("databaseName").collect():
      if self.domain in db["databaseName"] and self.layer in db["databaseName"] and self.env in db["databaseName"]:
        db_list.append(db["databaseName"])

    # exclude db patterns
    db_list = self._exclude_pattern(db_list,self.exclude_db_patterns)

    # fetch matching tables
    db_table_list = []
    for db in db_list:
      df = spark.sql("show tables from %s" %db)
      tables = []
      for table in df.select("tableName").collect():
        db_table = db + "." + table["tableName"]
        db_table_list.append(db_table)

    # exclude table patterns
    db_table_list = self._exclude_pattern(db_table_list,self.exclude_table_patterns)

    prefix = self._findcommonstart(db_table_list)
    table_ref_map = {}

    for table_name in db_table_list:
      new_table_ref = table_name.replace(prefix, '')
      table_ref_map[new_table_ref] = table_name

    return table_ref_map

  def get_latest_uuid(self):    
    """
    method type: public
    returns a dataframe with the latest UUID for the current object
    """
    current_uuid = (
      spark.table("prd_job_monitoring_db.uuid_registry_log")
      .where(col("cdl_service_name").like("%%%s%%" % self.domain))
    #   .where(col("business_description").like("%OBU%"))
      .orderBy(["cdl_effective_date","cdl_run_identifier"],ascending=False)
      .limit(1)
    )
    return current_uuid
  
  def get_dataset_methods(self):
    """
    method type: public
    returns list of data objects available for this Object
    """
    objs = zip(tuple(self.table_ref_map.keys()))
    cols = ["Objects"]

    df = (
            spark.createDataFrame(objs, cols)
            .sort(cols)
    )
    return df

  def get_data_frame(self,table_name):
#     print("Results for " + table_name)
    uuid_df = self.get_latest_uuid().withColumnRenamed('cdl_uuid', 'pt_cdl_uuid')
    uuid_df.printSchema()
    df = spark.table(table_name)
    
    #filter to current uuid
    df = (
      df.join(uuid_df, ["pt_cdl_uuid"])
    )

    return df
 

# COMMAND ----------

# MAGIC %md ### Customer class

# COMMAND ----------

class Customer(BaseObject):
  """
  Customer Data in iDNA
  """
  
  def __str__(self):
    message = """
      This class returns customer data.  
      To see all the tables available, call the get_dataset_methods() method
    """
    return message

  def __init__(self):
    self.domain = "customer"
    BaseObject.__init__(self, self.domain)
  
  # TODO make these methods show up in get_dataset_methods
  def account_basic_nominal(self):
    df = (
      self.basic_nominal()
      .where("customer_type = 'ACCOUNT'")
    )
    return df
    
  def professional_basic_nominal(self):
    df = (
      self.basic_nominal()
      .where("customer_type like '%PROFESSIONAL%'")
    )
    return df
  
#   TODO: figure out how to override a dynamic method
  def classification_universe(self, filter_condition = "classification_type = 'PR SF 5 ALL PORTFOLIO_TARGET'"):
    df = self.classification_universe()
    if filter_condition:
      df = df.where(filter_condition)
    
    return df

      

# COMMAND ----------

c = Customer().classification_universe(filter_condition = "classification_type = 'PR SF 5 ALL PORTFOLIO_TARGET'")


# COMMAND ----------

class test(Customer)

  def __init__(self):
    # Todo
    customer.__init__(self)
  
  def classification_universe()

# COMMAND ----------

# MAGIC %md ### Product class

# COMMAND ----------

class Product(BaseObject):
  """
  Product Data in iDNA
  """
  def __init__(self):
    self.domain = "product"
    super(Product, self).__init__(self.domain)

# COMMAND ----------

c = Product().get_dataset_methods()
display(c)

# COMMAND ----------

# MAGIC %md ## Test of Customer related methods

# COMMAND ----------

print(Customer())

# COMMAND ----------

c = Customer().get_dataset_methods2()

# COMMAND ----------

c = Customer().classification_universe()
display(c)

# COMMAND ----------

#1002390
c = Customer().professional_basic_nominal()
display(c)



# COMMAND ----------

c = Customer().account_basic_nominal()
display(c)

# COMMAND ----------

c = Customer().professional_basic_nominal()
display(c)

# COMMAND ----------

# MAGIC %md ## Test of Product related methods

# COMMAND ----------

Product().get_dataset_methods()

# COMMAND ----------

p = Product().basic_nominal()
display(p)

# COMMAND ----------

db_list = {}
df = spark.sql("show databases")
for db in df.select("databasename").collect():
  if "prd_published" in db["databasename"] and "ichub" not in db["databasename"] and "_service_" not in db["databasename"]:
      df2 = spark.sql("show tables from %s" % db["databasename"])
      for d in df2.select("database", "tablename").collect():
        if "unstitched_" not in d["tablename"] and "stitched_" not in d["tablename"]:
          print d["database"] + "\t" + d["tablename"]


# COMMAND ----------

# MAGIC %sql use prd_published_cdl_d_contract;show tables;

# COMMAND ----------

# MAGIC %sql describe formatted prd_published_cdl_d_contract.current_d_contract_nominal_customer;

# COMMAND ----------

x = dbutils.fs.ls("s3a://amgen-edl-gco-us-analytics-production/cdl/published/cdl/")
