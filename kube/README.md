# Déploiement sur Kubernetes (k8s)

## 1. Configuration

Ces commandes créent les variables sensibles et les configurations pour l'application :

```bash
# Créer les secrets (mots de passe, tokens, etc.)
kubectl apply -f secret.yaml

# Créer la configuration générale de l'application
kubectl apply -f configmap.yaml

# Créer le stockage persistant pour Neo4j
kubectl apply -f neo4j-pvc.yaml
```

## 2. Déployer la base de données Neo4j

Cette étape crée une instance de Neo4j avec stockage persistant :

```bash
# Déployer Neo4j en tant que StatefulSet (service avec état)
kubectl apply -f neo4j-statefulset.yaml

# Créer les services réseau pour accéder à Neo4j
kubectl apply -f services.yaml
```

## 3. Déployer le job

```bash
# Lancer les jobs (tâches) de l'application
kubectl apply -f job.yaml
```

## 4. Vérifier le déploiement

Vérifiez que tous les pods (conteneurs) sont en cours d'exécution :

```bash
# Surveiller l'état des pods en direct
kubectl get pods -n kuenz-adv-daba-26 -w
```

Le flag `-w` signifie "watch" (surveiller les changements en temps réel).

## Résumé complet en une seule commande

Pour supprimer tout l'existant :

```bash
kubectl delete -f .
```

Pour déployer tout d'un coup :

```bash
kubectl apply -f secret.yaml && \
kubectl apply -f configmap.yaml && \
kubectl apply -f neo4j-pvc.yaml && \
kubectl apply -f neo4j-statefulset.yaml && \
kubectl apply -f services.yaml && \
kubectl apply -f job.yaml
```

Puis surveillez le déploiement :

```bash
kubectl get pods -n kuenz-adv-daba-26 -w
```
