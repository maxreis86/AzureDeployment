import numpy as np
from flask import Flask, request, jsonify, render_template, url_for
import h2o
import pandas as pd
import json

app = Flask(__name__)
BestModelId = 'h2o_champion_titanic_propensity_survive_v1_20221127_154332.zip'

@app.route('/')
def home():
    return render_template('home.html')

@app.route('/predict',methods = ['POST'])
def predict():
    prediction = h2o.mojo_predict_pandas(pd.DataFrame(request.form.to_dict(), index=[0]),
                                     mojo_zip_path=BestModelId,
                                     genmodel_jar_path='h2o-genmodel.jar',
                                     verbose=False).loc[:,('predict','p1')]

    print(prediction['p1'][0])
    return render_template('home.html', prediction_text="AQI for Jaipur {}".format(prediction['p1'][0]))

if __name__ == '__main__':
    app.run(debug=True)