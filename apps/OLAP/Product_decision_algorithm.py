#!/usr/bin/env python
# coding: utf-8

import mysql.connector
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, classification_report
from sklearn.preprocessing import LabelEncoder

def collectp():
    # Se connecter à la base de données et extraire les données
    connection = mysql.connector.connect(
        host="localhost",
        user="root",
        password="",
        database="dw_bike_store"
    )
    
    query = """
    SELECT DISTINCT product_id, CA_by_product
    FROM retail_facts where product_id is not null
    """

    data = pd.read_sql_query(query, connection)
    connection.close()
    return data

def prepare_datap(data):
    # Ajoutez une nouvelle colonne 'product_category' avec la valeur par défaut 'Moyen'
    data['product_category'] = 'Moyen'

    # Identifiez les produits en fonction du chiffre d'affaires
    low_threshold = 10000
    high_threshold = 300000



    data.loc[data['CA_by_product'] > high_threshold, 'product_category'] = 'Élevé'
    data.loc[data['CA_by_product'] < low_threshold, 'product_category'] = 'Faible'
    
    # Encodez les catégories des produits
    label_encoder = LabelEncoder()
    data['product_category'] = label_encoder.fit_transform(data['product_category'])
    
    return data, label_encoder

def analyserp(data):
    # Divisez les données en ensembles d'entraînement et de test
    X = data.drop(columns=['product_id', 'CA_by_product'])
    y = data['product_category']
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    # Entraînez un modèle de classification (par exemple, un classificateur RandomForest)
    model = RandomForestClassifier(random_state=42)
    model.fit(X_train, y_train)

    return model, X_test, y_test


def agirp(model, X_test, y_test, label_encoder, data):
    # Faites des prédictions sur l'ensemble de test
    y_pred = model.predict(X_test)

    # Créez un DataFrame avec les informations des produits et les catégories prédites
    resultats = data.loc[X_test.index].copy()
    resultats['Product Category Prédite'] = label_encoder.inverse_transform(y_pred)
    
    # Évaluez les performances du modèle
    accuracy = accuracy_score(y_test, y_pred)
    report = classification_report(y_test, y_pred)
    
    return resultats, accuracy, report