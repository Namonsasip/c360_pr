#!/bin/bash bash
set -eux

kedro run --pipeline="customer_profile_to_l1_pipeline"
