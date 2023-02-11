from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import KFold
from sklearn import linear_model

kf = KFold(5, shuffle=True, random_state=48)
scaler = StandardScaler()


def logistic_regression(X, y):
    for train_ind, val_ind in kf.split(X, y):
        X_train, y_train = X[train_ind], y[train_ind]
        X_val, y_val = X[val_ind], y[val_ind]
        

        X_train_scale = scaler.fit_transform(X_train)
        X_val_scale = scaler.transform(X_val)

        # Logisitic Regression
        lr = LogisticRegression(
            class_weight= 'balanced',
            solver='newton-cg',
            fit_intercept=True,
        ).fit(X_train_scale, y_train)
    return lr, y_val, X_val_scale, scaler


def sgd_classifier(X, y):
    for train_ind, val_ind in kf.split(X, y):
        X_train, y_train = X[train_ind], y[train_ind]
        X_val, y_val = X[val_ind], y[val_ind]
        

        X_train_scale = scaler.fit_transform(X_train)
        X_val_scale = scaler.transform(X_val)


        sgd = linear_model.SGDClassifier(
            max_iter=1000,
            tol=1e-3,
            loss='log_loss',
            class_weight='balanced'
        ).fit(X_train_scale, y_train)    
    return sgd, y_val, X_val_scale, scaler




def sgd_huber_classifier(X, y):
    for train_ind, val_ind in kf.split(X, y):
        X_train, y_train = X[train_ind], y[train_ind]
        X_val, y_val = X[val_ind], y[val_ind]
        

        X_train_scale = scaler.fit_transform(X_train)
        X_val_scale = scaler.transform(X_val)


        sgd_huber = linear_model.SGDClassifier(
        max_iter=1000,
        tol=1e-3,
        alpha=20,
        loss='modified_huber',
        class_weight='balanced'
    ).fit(X_train_scale, y_train)
  
    return sgd_huber, y_val, X_val_scale, scaler


