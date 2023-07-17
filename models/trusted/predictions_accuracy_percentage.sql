{{ config(materialized='view') }}

WITH pred_accuracy as (
SELECT SUM(CASE WHEN p.prediction_accuracy = 'Correct Prediction' THEN 1 
       ELSE 0 
       END) as total_correct_predictions
    , COUNT(p.prediction_accuracy) as total_predictions
  FROM {{ ref('predictions_accuracy')}} p 
 GROUP BY ALL 
) 
SELECT total_correct_predictions
     , total_predictions
     , round((total_correct_predictions/total_predictions)*100,2)||'%' as pred_accuracy_percentage
  FROM pred_accuracy
  