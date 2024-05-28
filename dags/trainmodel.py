import pandas as pd
import numpy as np
from sklearn.cluster import KMeans
import matplotlib.pyplot as plt
import seaborn as sns

# For better visualization
sns.set()

# Load the data
data = pd.read_csv("/opt/airflow/dags/RFM.csv")

# Display the first few rows
print(data.head())
from sklearn.preprocessing import StandardScaler

# Selecting features for clustering
features = data[['Recency', 'Frequency', 'Monetary']]
data['Recency'] = data['Recency'].astype(float)
data['Frequency'] = data['Frequency'].astype(float)
data['Monetary'] = data['Monetary'].astype(float)

# Normalize the features
scaler = StandardScaler()
normalized_features = scaler.fit_transform(features)

# Elbow method to find the optimal number of clusters
sse = []
for k in range(1, 11):
    kmeans = KMeans(n_clusters=k, random_state=0)
    kmeans.fit(normalized_features)
    sse.append(kmeans.inertia_)

# # Plotting the results
# plt.figure(figsize=(10, 6))
# plt.plot(range(1, 11), sse, marker='o')
# plt.xlabel('Number of clusters')
# plt.ylabel('SSE')
# plt.title('Elbow Method For Optimal k')
# plt.show()

# Applying K-means with the chosen number of clusters
k = 4
kmeans = KMeans(n_clusters=k, random_state=0)
clusters = kmeans.fit_predict(normalized_features)

# Adding the cluster labels to the original dataframe
data['Cluster'] = clusters

# Visualization
# 2D pair plot
# sns.pairplot(data, hue='Cluster', palette='tab10')
# plt.show()

# # 3D plot for three features
# from mpl_toolkits.mplot3d import Axes3D

# fig = plt.figure(figsize=(10, 7))
# ax = fig.add_subplot(111, projection='3d')

# scatter = ax.scatter(
#     data['Recency'], data['Frequency'], data['Monetary'],
#     c=data['Cluster'], cmap='tab10', s=50
# )

# # Adding titles and labels
# ax.set_title('3D View of Customer Clusters')
# ax.set_xlabel('Recency')
# ax.set_ylabel('Frequency')
# ax.set_zlabel('Monetary')

# plt.colorbar(scatter)
# plt.show()