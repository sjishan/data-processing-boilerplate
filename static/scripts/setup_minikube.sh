cd ~/

apt-get update 

apt-get install -y apt-transport-https conntrack docker.io
systemctl enable --now docker

curl -Lo minikube https://storage.googleapis.com/minikube/releases/latest/minikube-linux-amd64
chmod +x minikube
mv ./minikube /usr/local/bin/minikube

curl -LO https://storage.googleapis.com/kubernetes-release/release/`curl -s https://storage.googleapis.com/kubernetes-release/release/stable.txt`/bin/linux/amd64/kubectl
chmod +x kubectl
mv ./kubectl /usr/local/bin/kubectl

cd -