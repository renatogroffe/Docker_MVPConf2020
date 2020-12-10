docker run -p 2345:80 -d renatogroffe/apicontagem-mvpconf:latest

docker run --name teste02 -e "MensagemVariavel=Teste via Ubuntu" -p 2346:80 -d renatogroffe/apicontagem-mvpconf:latest