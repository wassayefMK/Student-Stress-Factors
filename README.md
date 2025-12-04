# Student Stress Factors – Big Data Pipeline
This repository contains a complete **Big Data pipeline** for analyzing and predicting **student stress levels** using Apache Spark and AWS EMR. The project was developed as part of the **IT462 – Big Data Systems** course at King Saud University.
We use **RDD transformations**, **Spark SQL**, and **Spark MLlib** to explore stress-related factors, engineer insights, and train a machine learning model to classify students into **Low, Moderate, and High stress** categories.

## Project Overview
In academic environments, students are exposed to multiple psychological, social, and academic stressors. Early detection of stress can support timely intervention and improve well-being.
This project:
- Explores and preprocesses a survey-based stress dataset
- Uses Spark RDDs to derive lifestyle and stress patterns
- Applies Spark SQL to answer analytical questions
- Trains a Random Forest classifier to predict stress levels
- Demonstrates cloud deployment using AWS EMR

## Dataset – Student Stress Factors
We use the Student Stress Factors: A Comprehensive Analysis dataset from Kaggle: https://www.kaggle.com/datasets/rxnach/student-stress-factors-a-comprehensive-analysis
The datasets contain:
- 1100 students
- 21 attributes covering:
  - Psychological: anxiety, depression, self-esteem  
  - Physiological: headaches, blood pressure, sleep quality, breathing problems  
  - Social: social support, peer pressure, bullying, extracurricular activities  
  - Environmental: noise level, living conditions, safety, basic needs  
  - Academic: academic performance, study load, teacher–student relationship  
- Target variable: `stress_level`

## Pipeline & Methodology

### Data Exploration & Preprocessing (Python / Spark)
- Checked missing values, duplicates, and value ranges – no missing or duplicate rows found. 
- Applied Min–Max normalization to bring all numerical features to a comparable scale. 
- Verified multicollinearity using a correlation matrix and heatmap (no highly correlated pairs above 0.80). 

### RDD Operations (Spark Core)
We used RDD transformations and actions to extract insights:
- Categorized students into **Low / Moderate / High stress** groups.  
- Filtered vulnerable students (High stress + low social support). 
- Grouped by academic performance and computed average stress.  
- Built lifestyle clusters based on sleep, study load, support, performance, teacher–student relationship, and peer pressure.
- Computed sleep-to-study ratio and analyzed balanced vs. extreme lifestyles.  

These operations helped uncover patterns such as:
- High stress among students with **low support and poor performance**  
- Extremes in sleep-to-study ratio associated with risk of poor outcomes 

### Spark SQL Analytics
Using Spark SQL, we answered questions like:
- How do extracurricular activities affect study load, stress, and performance?  
- How do future career concerns relate to stress and academic achievement?  
- Is the teacher–student relationship related to workload and performance?  
- Does social support from friends reduce stress and mental health issues?  
- How does sleep quality influence stress and academic outcomes?  

The queries revealed, for example, that:
- Better teacher–student relationships are linked to lower stress and better performance
- Higher social support is associated with less stress and fewer mental health issues
- Poor sleep quality correlates with higher stress and worse performance

### Machine Learning – Random Forest (Spark MLlib)
We framed stress prediction as a multiclass classification problem with three classes (0, 1, 2). 
Steps:
1. Feature Vectorization
   - Combined 20 numerical features into a `features` vector using `VectorAssembler`.

2. Model & Pipeline
   - `RandomForestClassifier` within a Spark `Pipeline`.

3. Hyperparameter Tuning (Cross-Validation)
   - `numTrees`: {10, 20, 30}  
   - `maxDepth`: {3, 5, 7}  
   - 5-fold cross-validation.

4. Train/Test Split 
   - 70% training, 30% testing.

#### Results
- **Accuracy:** 0.90  
- **Precision:** 0.9002  
- **Recall:** 0.90  
- **Test Error:** 0.10 

Best model:
- `numTrees = 20`  
- `maxDepth = 3`  
The model achieves **high, balanced performance**, suggesting it is suitable for early stress detection and for further deployment. 

## Cloud Deployment – AWS EMR
To run the pipeline in a scalable way, we used **AWS EMR**: 
- **EMR Release:** 7.8.0  
- **Spark:** 3.5.4  
- **Hadoop:** 3.4.1  
- **Cluster:**  
  - 1 Primary node (m5.xlarge)  
  - 1 Core node (m5.xlarge)  
  - 1 Task node (m5.xlarge)  
- **Storage:** 15 GiB SSD (gp3)  
- Input data in S3:  
  `s3://student-stress-analysis-bucket/cleaned_student_stress_factors.csv`
We executed the Spark job via a **Custom JAR step** using `command-runner.jar` and `spark-shell`, and wrote evaluation results (accuracy, test error, best model config) back to S3.

## Key Takeaways

- Many stress factors are multi-dimensional, combining academic, social, and psychological components.
- RDD and SQL analyses highlight the importance of:
-- Social support
-- Sleep quality
-- Teacher–student relationships
- A tuned Random Forest model achieves ~90% accuracy, making it a strong candidate for early stress detection systems.
- Cloud execution via AWS EMR enables scalable and reproducible big data workflows.
