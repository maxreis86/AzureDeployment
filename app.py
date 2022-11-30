import numpy as np
from flask import Flask, request, jsonify, render_template, url_for
import h2o
import pandas as pd
import json
import re
import os

#install java
os.system('apt install -y openjdk-8-jdk')

app = Flask(__name__)
BestModelId = 'h2o_champion_titanic_propensity_survive_v1_20221127_154332.zip'

@app.route('/')
def home():
    return render_template('home.html')

@app.route('/predict',methods = ['POST'])
def predict():
    
    dataprep_df  = pd.DataFrame(request.form.to_dict(), index=[0])
    
    #criar o prefix para a variavel cabine (A, B, C, D, ou E)
    dataprep_df['cabine_prefix'] = dataprep_df['Cabin'].str[0:1]
    dataprep_df = dataprep_df.fillna(value={'cabine_prefix': 'missing'})
    
    #Tiket    
    for i in range(len(dataprep_df)):
        t=dataprep_df.loc[i:i,'Ticket'].values
        t2=re.sub("[^0-9]", "", str(t))
        t3=re.sub('[^A-Za-z]+', '', str(t))
        if t2 != "":
            dataprep_df.loc[i:i,'Ticket_int']=int(t2)
        if t3 != "":
            if t3 == 'SC':
                t3 = 'SCAHBasle'
            if t3 == 'SOP':
                t3 = 'SOPP'
            if t3 == 'C':
                t3 = 'CA'
            if t3 == 'FC':
                t3 = 'FCC'
            if t3 == 'PP':
                t3 = 'PPP'
            if t3 == 'SCOW':
                t3 = 'Fa'
            if t3 in ('AS', 'CASOTON', 'Fa', 'SCA','SOPP','SOTONO','SP'):
                t3='LOW'
        dataprep_df.loc[i:i,'Ticket_str']=str(t3)

    dataprep_df['Ticket_int'] = dataprep_df['Ticket_int'].fillna(0)
    dataprep_df = dataprep_df.fillna(value={'Ticket_str': 'missing'})
    
    #Name: Criar uma categoria com o titulo do nome
    for i in range(len(dataprep_df)):
        t1 = str(dataprep_df.loc[i:i,'Name'].values)
        t2 = t1[0:t1.find('.')].split()[-1]
        if t2 in ('Rev', 'Capt', 'Don', 'Jonkheer'):
            t2='LOW'
        if t2 in ('Lady', 'Mme'):
            t2='Miss'
        dataprep_df.loc[i:i,'NameTitle']=str(t2)
        
    #Create the "Missing" category for missing values in string vaviables
    dataprep_df = dataprep_df.apply(lambda x: x.fillna(np.nan) if x.dtype.kind in 'biufc' else x.fillna('Missing'))
    
    dataprep_df = dataprep_df.set_index('Embarked').drop(columns=['Ticket','Name','Cabin'])

    prediction = h2o.mojo_predict_pandas(dataprep_df,
                                     mojo_zip_path=BestModelId,
                                     genmodel_jar_path='h2o-genmodel.jar',
                                     verbose=False).loc[:,('predict','p1')]

    print(prediction['p1'][0])
    return render_template('home.html', prediction_text="Probability of surviving {:.0%}".format(prediction['p1'][0]))

if __name__ == '__main__':
    app.run(debug=True)