import joblib

class SentimentModel:
    def __init__(self):
        self.model = joblib.load("sentiment_model.joblib")
        self.vectorizer = joblib.load("vectorizer.joblib")

    def predict(self, text: str):
        X = self.vectorizer.transform([text])
        pred = self.model.predict(X)[0]
        prob = self.model.predict_proba(X).max()
        return pred, float(prob)
