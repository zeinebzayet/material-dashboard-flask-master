#!/usr/bin/env python
# coding: utf-8

# In[1]:
import mysql.connector
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, classification_report
from sklearn.preprocessing import LabelEncoder

def collect():
    # Se connecter à la base de données et extraire les données
    connection = mysql.connector.connect(
        host="localhost",
        user="root",
        password="",
        database="dw_bike_store"
    )
    
    query = """
    SELECT o.customer_id,
       c.first_name,
       c.last_name,
       c.phone,
       r.total_sale_by_customer, 
       MIN(o.order_date_year) AS most_old_order_year
    FROM orders o
    LEFT JOIN retail_facts r ON o.customer_id = r.customer_id
    LEFT JOIN customers c ON o.customer_id = c.customer_id
    GROUP BY o.customer_id


    """

    data = pd.read_sql_query(query, connection)
    connection.close()

    return data

def prepare_data(data):
    # Add a new column 'customer_segment' with default value 'Nouveau'
    data['customer_segment'] = 'Nouveau'

    # Identify customers who meet the 'Fidele' criteria
    fidele_mask = (data['most_old_order_year'] <= 2022) &                   (data['total_sale_by_customer'] >= 5000)
    # Set 'customer_segment' to 'Fidele' for customers who meet the criteria
    data.loc[fidele_mask, 'customer_segment'] = 'Fidele'
    
    # Encode categorical columns 'first_name' and 'last_name'
    #label_encoder = LabelEncoder()
    #data['first_name'] = label_encoder.fit_transform(data['first_name'])
    #data['last_name'] = label_encoder.fit_transform(data['last_name'])
    #data['phone'] = label_encoder.fit_transform(data['phone'])

    # Remove non-numeric columns that can't be used as features
    data = data.drop(columns=['total_sale_by_customer'])
    
    # Encode the 'customer_segment' column
    label_encoder = LabelEncoder()
    data['customer_segment'] = label_encoder.fit_transform(data['customer_segment'])
    
    return data, label_encoder

def analyser(data):
    # Diviser les données en ensembles d'entraînement et de test
    X = data.drop(columns=['customer_segment', 'first_name', 'last_name', 'phone'])
    y = data['customer_segment']
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    # Entraîner un modèle de classification (par exemple, un classificateur RandomForest)
    model = RandomForestClassifier(random_state=42)
    model.fit(X_train, y_train)

    return model, X_test, y_test

def agir(model, X_test, y_test, label_encoder, data):
    # Faire des prédictions sur l'ensemble de test
    y_pred = model.predict(X_test)

    # Créer un DataFrame avec les informations des clients et les segments prédits
    resultats = data.loc[X_test.index].copy()
    resultats['Segment Prédit'] = label_encoder.inverse_transform(y_pred)
    
    # Évaluer les performances du modèle
    accuracy = accuracy_score(y_test, y_pred)
    report = classification_report(y_test, y_pred)

    return resultats, accuracy, report