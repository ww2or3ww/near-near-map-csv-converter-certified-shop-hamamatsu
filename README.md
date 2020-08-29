# near-near-map-csv-converter-certified-shop-hamamatsu

## ライブラリのインストール
$ pip install -r requirements.txt -t source  

## パッケージング&デプロイ コマンド
$ find . | grep -E "(__pycache__|\.pyc|\.pyo$)" | xargs rm -rf  
$ cd source  
$ zip -r ../lambda-package.zip *  
$ aws lambda update-function-code --function-name {{your function name}} --zip-file fileb://../lambda-package.zip  
