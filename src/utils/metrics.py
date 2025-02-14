"""
Metrics collection utilities for ETL job monitoring.
"""

import time
from typing import Dict, List, Optional
from dataclasses import dataclass, field
from datetime import datetime
import json
import boto3

@dataclass
class EntityMetric:
    """Metric data for entity processing."""
    data_owner: str
    context: str
    processing_time: float
    timestamp: str = field(default_factory=lambda: datetime.now().isoformat())

@dataclass
class FileMetric:
    """Metric data for file processing."""
    file_name: str
    processing_time: float
    entity_count: int
    timestamp: str = field(default_factory=lambda: datetime.now().isoformat())

@dataclass
class JobMetric:
    """Metric data for overall job execution."""
    total_time: float
    files_processed: int
    total_files: int
    timestamp: str = field(default_factory=lambda: datetime.now().isoformat())

class MetricsCollector:
    """Collects and manages metrics for the ETL job."""
    
    def __init__(self):
        """Initialize metrics collector."""
        self.entity_metrics: List[EntityMetric] = []
        self.file_metrics: List[FileMetric] = []
        self.job_metrics: List[JobMetric] = []
        self.cloudwatch = boto3.client('cloudwatch')
        
    def record_entity_processing(self, data_owner: str, context: str, processing_time: float) -> None:
        """Record metrics for entity processing.
        
        Args:
            data_owner: Name of the data owner
            context: Processing context (STOCK/SALES)
            processing_time: Time taken to process the entity in seconds
        """
        metric = EntityMetric(data_owner, context, processing_time)
        self.entity_metrics.append(metric)
        
        # Send to CloudWatch
        self.cloudwatch.put_metric_data(
            Namespace='FileStandardization',
            MetricData=[
                {
                    'MetricName': 'EntityProcessingTime',
                    'Value': processing_time,
                    'Unit': 'Seconds',
                    'Dimensions': [
                        {'Name': 'DataOwner', 'Value': data_owner},
                        {'Name': 'Context', 'Value': context}
                    ]
                }
            ]
        )
        
    def record_file_processing(self, file_name: str, processing_time: float, entity_count: int) -> None:
        """Record metrics for file processing.
        
        Args:
            file_name: Name of the processed file
            processing_time: Time taken to process the file in seconds
            entity_count: Number of entities in the file
        """
        metric = FileMetric(file_name, processing_time, entity_count)
        self.file_metrics.append(metric)
        
        # Send to CloudWatch
        self.cloudwatch.put_metric_data(
            Namespace='FileStandardization',
            MetricData=[
                {
                    'MetricName': 'FileProcessingTime',
                    'Value': processing_time,
                    'Unit': 'Seconds',
                    'Dimensions': [
                        {'Name': 'FileName', 'Value': file_name}
                    ]
                },
                {
                    'MetricName': 'EntityCount',
                    'Value': entity_count,
                    'Unit': 'Count',
                    'Dimensions': [
                        {'Name': 'FileName', 'Value': file_name}
                    ]
                }
            ]
        )
        
    def record_job_completion(self, total_time: float, files_processed: int, total_files: int) -> None:
        """Record metrics for job completion.
        
        Args:
            total_time: Total job execution time in seconds
            files_processed: Number of successfully processed files
            total_files: Total number of files attempted
        """
        metric = JobMetric(total_time, files_processed, total_files)
        self.job_metrics.append(metric)
        
        # Send to CloudWatch
        self.cloudwatch.put_metric_data(
            Namespace='FileStandardization',
            MetricData=[
                {
                    'MetricName': 'JobExecutionTime',
                    'Value': total_time,
                    'Unit': 'Seconds'
                },
                {
                    'MetricName': 'FilesProcessed',
                    'Value': files_processed,
                    'Unit': 'Count'
                },
                {
                    'MetricName': 'ProcessingSuccessRate',
                    'Value': (files_processed / total_files * 100) if total_files > 0 else 0,
                    'Unit': 'Percent'
                }
            ]
        )
        
    def get_summary(self) -> Dict:
        """Get a summary of all collected metrics.
        
        Returns:
            Dict containing summarized metrics
        """
        return {
            'job_metrics': [vars(m) for m in self.job_metrics],
            'file_metrics': [vars(m) for m in self.file_metrics],
            'entity_metrics': [vars(m) for m in self.entity_metrics],
            'summary': {
                'total_entities': len(self.entity_metrics),
                'total_files': len(self.file_metrics),
                'average_entity_time': sum(m.processing_time for m in self.entity_metrics) / len(self.entity_metrics) if self.entity_metrics else 0,
                'average_file_time': sum(m.processing_time for m in self.file_metrics) / len(self.file_metrics) if self.file_metrics else 0
            }
        }
        
    def flush(self) -> None:
        """Flush metrics to persistent storage."""
        try:
            # Save metrics to S3 or another persistent store
            metrics = self.get_summary()
            s3 = boto3.client('s3')
            
            # Save to S3 with timestamp in filename
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            key = f'metrics/file_standardization_{timestamp}.json'
            
            s3.put_object(
                Bucket='your-metrics-bucket',  # Replace with your bucket
                Key=key,
                Body=json.dumps(metrics, indent=2),
                ContentType='application/json'
            )
        except Exception as e:
            # Log error but don't fail the job
            print(f"Failed to flush metrics: {str(e)}") 