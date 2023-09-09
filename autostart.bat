REM inicializando a api
start "iniciando_api_fii" cmd.exe @cmd /k "python c:\Users\Isaac\Documents\python_workspace\api_fiis\index.py"
REM aguandando a correta inicializacao da api
sleep 5
REM fazendo uma requisicao para alimentar os dados no banco de dados 
curl http://localhost:5000/fii_data
REM fechando a api
taskkill /FI "WindowTitle eq iniciando_api_fii*" /T /F
exit