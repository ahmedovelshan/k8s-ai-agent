# This guid contains information about how to create AI agent for you cluster

Step 1:
Create new NodeGroup for AI Agent.
  eksctl create nodegroup -f nodegroup-ai-agent.yaml
Once that finishes, confirm with:
  kubectl get nodes -l workload=ai-agent

Step 2:
Deploying Ollama + Phi-3.5
  kubectl apply -f 00-namespace.yaml
  kubectl apply -f 01-ollama.yaml

Wait for pod to be Running
kubectl get pods -n ai-ops-agent -w

Once the pod is Running, pull the model into it:
  kubectl exec -n ai-ops-agent deploy/ollama -- ollama pull phi3.5:3.8b
This downloads ~2.2GB — give it a few minutes depending on network. Then verify it's ready:
  kubectl exec -n ai-ops-agent deploy/ollama -- ollama list

Smoke test
  kubectl exec -n ai-ops-agent deploy/ollama -- ollama run phi3.5:3.8b "A Kubernetes pod is in CrashLoopBackOff. What are the top 3 things to check? Answer in a short list."
  time kubectl exec -n ai-ops-agent deploy/ollama -- ollama run phi3.5:3.8b "Say OK if you're working."

Step 3:
The Detector

Create ECR repo (one-time)
  aws ecr create-repository --repository-name ai-ops-agent/detector --region eu-central-1

Get your account ID for the registry URL
  aws sts get-caller-identity --query Account --output text

Build and push
  cd ai-ops-agent/detector
  aws ecr get-login-password --region eu-central-1 | docker login --username AWS --password-stdin <ACCOUNT_ID>.dkr.ecr.eu-central-1.amazonaws.com

  docker build -t ai-ops-agent/detector .
  docker tag ai-ops-agent/detector:latest <ACCOUNT_ID>.dkr.ecr.eu-central-1.amazonaws.com/ai-ops-agent/detector:latest
  docker push <ACCOUNT_ID>.dkr.ecr.eu-central-1.amazonaws.com/ai-ops-agent/detector:latest

Then edit detector-deployment.yaml, replacing REPLACE_WITH_YOUR_ECR_IMAGE_URI with that pushed image URI, and apply:
  kubectl apply -f rbac-detector.yaml
  kubectl apply -f detector-deployment.yaml

  kubectl get pods -n ai-ops-agent -l app=detector
  kubectl logs -n ai-ops-agent -l app=detector -f

Quick test
  kubectl logs -n ai-ops-agent -l app=detector -f
  kubectl run crashtest --image=busybox --restart=Always -- sh -c "echo 'simulated failure: config file missing' && exit 1"
  kubectl delete pod crashtest --ignore-not-found




