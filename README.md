# Big-Data
# Financial Asset Recommendation Platform – Big Data Project

This repository contains my implementation of a **financial recommendation platform** built using **Apache Spark** for big data analytics.

The goal of the project was to develop a scalable, distributed pipeline that analyzes historical stock market data to identify the **top 5 most profitable assets** for investment, based on a combination of technical indicators and financial metrics.

## Project Overview

Given a large dataset of pricing data and asset metadata from the US stock market (covering 1999 to mid-2020), I designed a pipeline that:

1. Loads and cleanses the pricing and metadata into **Resilient Distributed Datasets (RDDs)**.
2. Computes key **technical indicators** (including return on investment and volatility) for each asset.
3. Filters assets based on:
   - Volatility threshold (removing assets with volatility ≥ 4).
   - Price-to-Earnings (P/E) ratio threshold (removing assets with P/E ≥ 25).
4. Ranks the remaining assets based on their **5-day return on investment**, producing a final **top 5 ranking**.

All computations were implemented as **distributed Spark transformations and actions**, adhering to object-oriented principles in Java.

## 🛠️ Tools & Technologies

- **Apache Spark 4.0.0-preview2**
- **Apache Hadoop (HDFS integration)**
- **Java JDK 21.0.2**
- **IntelliJ IDEA**

The project leverages Spark and Hadoop to handle large-scale data processing efficiently in a distributed environment.

##  Dataset

- `all_prices-noHead.csv` (~2.4GB): daily closing prices for ~15,700 financial assets.
- `stock_data.json`: metadata including name, sector, industry, and P/E ratio.

The dataset covers millions of records, enabling a realistic simulation of large-scale financial data processing.

## ⚙️ Key Components

Some of the main components I developed and worked with include:

- **Data Loading and Preprocessing**  
  Handled large-scale CSV and JSON files, filtering out incomplete or missing data.
  
- **Technical Indicator Computation**  
  Integrated provided classes to calculate **volatility** (using 251-day historical window) and **returns** (5-day window).
  
- **Filtering Logic**  
  Applied sequential filtering based on volatility and P/E ratio constraints.
  
- **Ranking Mechanism**  
  Sorted and selected the top 5 assets with the highest recent returns.

The implementation makes use of Spark’s **JavaRDD** and **Java SQL Dataset APIs**, balancing between functional transformations and clarity of code without heavy reliance on lambdas, to ensure readability and maintainability.

## 💭 Reflections

This project was a great opportunity to apply distributed computing concepts to a **real-world financial analytics task**. It allowed me to explore:

Building data pipelines at scale  
Working with heterogeneous data formats  
Applying domain-specific filtering and ranking logic  
Writing efficient and readable Spark applications in Java

I also gained deeper appreciation for balancing **performance, code quality, and scalability** when working with big data systems.

---

Feel free to explore the code and reach out if you're interested in discussing big data analytics, financial data processing, or Spark development!

