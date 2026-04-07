##!/usr/bin/env bash

ssh -o StrictHostKeyChecking=no -A -i ~/.ssh/csci1380.pem ubuntu@"$1"
