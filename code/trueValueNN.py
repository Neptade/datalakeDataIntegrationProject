import json
import flask
from flask import request, jsonify
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
import torch
from torch import nn

# Set device
device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

# Configure Spark
conf = SparkConf().setMaster("spark://spark-master:7077").set("spark.shutdownhookmanager.enabled", "false")
spark = SparkSession.builder.config(conf=conf).appName("weatherData").getOrCreate()

# Initialize Flask app
app = flask.Flask(__name__)
app.config["DEBUG"] = True

# Define the neural network
class NeuralNetwork(nn.Module):
    def __init__(self):
        super().__init__()
        self.linear_relu_stack = nn.Sequential(
            nn.Linear(6, 128), 
            nn.ReLU(),
            nn.Linear(128, 128), 
            nn.ReLU(),
            nn.Linear(128, 1)
        )
    
    def forward(self, x):
        return self.linear_relu_stack(x)

# Load the model
model = NeuralNetwork()
model.load_state_dict(torch.load('model.pth'))
model.to(device)
model.eval()

# Define prediction route
@app.route('/predict', methods=['POST'])
def predict():
    # Get JSON data from request
    input_data = request.json
    
    # Convert input data to DataFrame
    df = pd.DataFrame(input_data)
    
    # Convert DataFrame to PyTorch tensor
    tensor_data = torch.tensor(df.values, dtype=torch.float32).to(device)
    
    # Make predictions
    with torch.no_grad():
        predictions = model(tensor_data).cpu().numpy().tolist()
    
    # Return predictions as JSON
    return jsonify(predictions)

# Run the Flask app
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)