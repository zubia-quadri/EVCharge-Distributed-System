#!/bin/bash
for p in $(kubectl get pods -l app=worker -o name); do
    count=$(kubectl logs $p | grep -o "Handling request" | wc -l)
    echo "$p: $count requests"
done
