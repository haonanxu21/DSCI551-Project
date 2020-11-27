from flask import *
import requests
import pandas as pd
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql import SQLContext
import re


sc = SparkContext('local')
sqlContext = SQLContext(sc)
spark = SparkSession(sc)

url = 'https://ds-demo-c0812.firebaseio.com/data_engineer.json'
response = requests.get(url)
dict_eng = response.json()
eng_df = pd.read_json(json.dumps(dict_eng))

eng_spark_df = sqlContext.createDataFrame(eng_df)

eng_df = eng_spark_df.toPandas()

def salary_range(df, sal):
    ############################################################
    # Method: salary_range(df, sal)
    # input: 
    #   df: a pandas df
    #   sal: the target salary input by user
    # return:
    #   A new pandas df with extra needed columns
    ##################### Psuedo Code ##########################
    # read a df with df['Salary Estimate']==xxK-xxK
    # replace K with 000, split by "-", to int, append to lists
    # append to df['lower'] & df['uppeer']
    # check upper and lower and boolean df['inRange']
    # to spark and filter, toPandas and return
    lower = []
    upper = []
    for i in range(len(df)):
        tmp = df['Salary Estimate'][i]
        tmp2 = tmp.replace('K', '000')
        sal_range = re.split('\\$|-',tmp2) # s[1]&s[3] are numbers
        lower.append(sal_range[1])
        upper.append(sal_range[3])

    df['lower'] = lower
    df['upper'] = upper
    inRange = []
    #salcheck = [] # debug use
    for j in range(len(df)):
        if( (sal>=float(df['lower'][j])) & (sal<=float(df['upper'][j])) ):
            inRange.append(True)
            #salcheck.append(type(sal)) # debug use
        else:
            inRange.append(False)

    df['inRange'] = inRange
    #df['debug'] = salcheck # debug use
    tmp_spark_df = sqlContext.createDataFrame(df)
    tmp2_spark_df = tmp_spark_df.filter(tmp_spark_df['inRange']==True).select('Company Name','Salary Estimate','Location','Size','Sector')
    new_df = tmp2_spark_df.toPandas()
    return new_df


app = Flask(__name__)

@app.route('/', methods=['GET', 'POST'])

def basic():
    if request.method == 'POST':
        if request.form['submit'] == 'overview':
            test = request.form['name']
            try:
                num = int(test)
                eng_spark_head_df = eng_spark_df.select('Company Name','Salary Estimate','Location','Size','Sector').limit(num)
                eng_head_df = eng_spark_head_df.toPandas()
                eng_head_df['Company Name'] = eng_head_df['Company Name'].str.slice(0,-4,1)
                eng_head_df['Salary Estimate'] = eng_head_df['Salary Estimate'].str.slice(0,-17,1)
                eng_head_df = eng_head_df.replace('-1', 'Unknown')
                return render_template('index.html', tables=[eng_head_df.to_html(classes='data')], titles=eng_head_df.columns.values)
            except:
                return render_template('index.html',tables=["Please enter a positive integer."], titles=None)
        
        elif request.form['submit'] == 'location':
            name = request.form['name']
            # Use spark to select all location == input
            # remember to remove the limit 
            eng_spark_location_df = eng_spark_df.filter(eng_spark_df["Location"]==name).select('Company Name','Salary Estimate','Location','Size','Sector')#.limit(30)
            eng_location_df = eng_spark_location_df.toPandas()
            # Perform necessary data cleaning for readability
            eng_location_df['Company Name'] = eng_location_df['Company Name'].str.slice(0,-4,1)
            eng_location_df['Salary Estimate'] = eng_location_df['Salary Estimate'].str.slice(0,-17,1)
            eng_location_df = eng_location_df.replace('-1', 'Unknown')
            eng_location_lst = eng_location_df.values.tolist()
            #output=[['Company Name','Salary Range','Location','Company Size']]
            #for item in eng_location_lst:
            #    output.append(item)
            if eng_location_lst==[]:
                return render_template('index.html',tables=["No match or invalid input!"], titles=None)

            return render_template('index.html', tables=[eng_location_df.to_html(classes='data')], titles=eng_location_df.columns.values)
        
        elif request.form['submit'] == 'salary':
            salary = request.form['name']
            # check if input is a number, if not, return
            try:
                checkFormat = float(salary)
            except:
                return render_template('index.html',tables=['Invalid input! Please enter a number.'], titles=None)
            # Use spark to select all location == input
            eng_spark_salary_df = eng_spark_df.select('Company Name','Salary Estimate','Location','Size','Sector')
            eng_salary_df = eng_spark_salary_df.toPandas()
            # Perform necessary data cleaning for readability
            eng_salary_df['Company Name'] = eng_salary_df['Company Name'].str.slice(0,-4,1)
            eng_salary_df['Salary Estimate'] = eng_salary_df['Salary Estimate'].str.slice(0,-17,1)
            eng_salary_df = eng_salary_df.replace('-1', 'Unknown')
            # Find suitable salary range
            eng_new_salary_df = salary_range(eng_salary_df, float(salary))
            eng_salary_lst = eng_new_salary_df.values.tolist()
            #output=[['Company Name','Salary Range','Location','Company Size']]
            #for item in eng_salary_lst:
            #    output.append(item)
            if eng_salary_lst==[]:
                return render_template('index.html',tables=["No match!"], titles=None)
            
            return render_template('index.html',tables=[eng_new_salary_df.to_html(classes='data')], titles=eng_new_salary_df.columns.values)

        elif request.form['submit'] == 'sector':
            name = request.form['name']
            # Use spark to select all location == input
            eng_spark_sector_df = eng_spark_df.filter(eng_spark_df["Sector"]==name).select('Company Name','Salary Estimate','Location','Size','Sector')
            eng_sector_df = eng_spark_sector_df.toPandas()
            # Perform necessary data cleaning for readability
            eng_sector_df['Company Name'] = eng_sector_df['Company Name'].str.slice(0,-4,1)
            eng_sector_df['Salary Estimate'] = eng_sector_df['Salary Estimate'].str.slice(0,-17,1)
            eng_sector_df = eng_sector_df.replace('-1', 'Unknown')
            eng_sector_lst = eng_sector_df.values.tolist()
            if eng_sector_lst==[]:
                return render_template('index.html', tables=["No match or be more specific.","For example: Finance, Infromation Technology, etc."], titles=None)

            return render_template('index.html',tables=[eng_sector_df.to_html(classes='data')], titles=eng_sector_df.columns.values)

    return render_template('index.html')


if __name__ == '__main__':
	app.run()
