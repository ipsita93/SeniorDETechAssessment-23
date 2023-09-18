
# Section3: System Design

## Design 2

This architectural design ensures that the image processing pipeline is secure, scalable, cost-effective, and compliant with data retention policies. It leverages cloud services to provide high availability, fault tolerance, and efficient processing of image data.

## How does it work? 
When an HTTP request is received from a web app, it is passed from CloudFront to API Gateway, and then forwarded to the Lambda function for processing. If the image is cached by CloudFront because of an earlier request, CloudFront will return the cached image instead of forwarding the request to the API Gateway. This reduces latency and eliminates the cost of reprocessing the image.

Requests that are not cached are passed to the API Gateway, and the entire request is forwarded to the Lambda function. The Lambda function retrieves the original image from your Amazon S3 bucket and uses Sharp (the open source image processing software) to return a modified version of the image to the API Gateway, which then stores a copy in the CloudFront cache and then returns to the client web app. 

## Tech Stack

#### Components ####
- Confluent Cloud
- Amazon CloudFront
- Amazon API Gateway
- AWS Lambda
- Amazon S3 Bucket
- PowerBI 

#### Benefits ####
- Managability
- Scalability
- Highly Secure
- High Availability
- Elastic
- Fault Tolerant and Disaster Recovery
- Efficient
- Low Latency
- Least Privilege

## Architecture Diagram  - Detailed Explanation 
*See design2_architecture_diagram.draw.io or design2_architecture_diagram.png* 

#### 1a. Confluent Cloud (Engineering Team Managed Kafka Stream): 
- A separate web application hosting a Kafka stream for uploading images to the cloud.
- Managed by company engineers for scalability and reliability. 
- Uses HTTPS for secure data transfer to ensure encryption of data. 

#### 1b. Web App (used by User):
- A web application that allows users to upload images to the cloud via a secure API.
- Uses HTTPS for secure data transfer to ensure encryption of data. 
 
#### 2. Amazon CloudFront distribution: 
- Provides a caching layer to reduce the cost of image processing and the latency of subsequent image delivery. 
- The CloudFront domain name provides cached access to the image handler API.

#### 3. Amazon API Gateway: 
- Provide endpoint resources and initiate the AWS Lambda function.
- API Gateway handles all the tasks involved in accepting and processing up to hundreds of thousands of concurrent API calls, including traffic management, CORS support, authorization and access control, throttling, monitoring, and API version management.
- API Gateway has no minimum fees or startup costs. You pay for the API calls you receive and the amount of data transferred out and, with the API Gateway tiered pricing model, you can reduce your cost as your API usage scales.

#### 4. AWS Lambda: 
- To run code for image manipulation without the need for provisioning or managing servers (thereby reducing costs and overhead). Serverless functions responsible for automatically processing images when they arrive. Simply write and upload code as a .zip file or container image.
- Automatically respond to code execution requests at any scale, from a dozen events per day to hundreds of thousands per second.
- Save costs by paying only for the compute time you use—by the millisecond—instead of provisioning infrastructure upfront for peak capacity.
- Optimize code execution time and performance with the right function memory size. Respond to high demand in double-digit milliseconds with Provisioned Concurrency. Scales automatically based on incoming workload.

#### 5. Amazon S3 Bucket: 
- Stores both processed images for immediate access and archived images (less than 7 days) for compliance. 
- Stores system logs separately. 
- Secure: Data at rest encrypted using cloud provider's encryption services to ensure security. 
- Cost-effective: Lifecycle policies move archived images to lower-cost storage and purge them after 7 days. To increase the security further, we can use the following options: 
    - AWS Identity and Access Management (IAM) - Create new users and assign different access to them.
    - Access Control Lists (ACLs) - Make individual objects accessible to authorized users.
    - Bucket Policies - Configure access policies to all objects within a bucket.
    - Block Public Access - Block public access to all objects at bucket level or account level.
    - Object Lock - Blocks object version deletion for a defined retention period.
    - Audit Logs - Lists all the requests made for S3 resources.
    - Query String Authentication - Limited access using temporary URLs.
- Elastic and Durable: Scale storage resources to meet fluctuating needs with 99.9% of data durability.
- High Availability & Fault Tolerant & Disaster Recovery: Although AWS S3 is a global service, it also uses the availability zones to ensure the high availability of the data. It replicates data across 3 availability zones in a region and makes sure the data is available even in a physical disaster. All the S3 classes are designed to retain the data even if a complete availability zone is lost. 
- Can also use object versioning to recover in case of intentional or unintentional deletion of the data.
- Scalability: Supports parallel requests and performance scales per prefix. For example, if the application can perform 5000 GET requests per second/prefix, we can quickly scale it 10 times by creating 10 prefixes within the bucket.
- Low Latency: Regardless of the scale, S3 ensures high performance for the application's needs with a latency of around 100-200 milliseconds.

#### 6. PowerBI (Cloud-based database for business intelligence use case): 
- A dedicated resource for company analysts to perform analytical computations on stored image metadata.
- Secure access control to restrict access to authorized personnel  only (BI team and leadership). 
- AWS Lamda can write metadata into the business intelligence database after it writes into S3 storage. 

#### Security and Access Control:
- Role-based access control (RBAC) for fine-grained permission management.
- Multi-factor authentication (MFA) for enhanced security.

#### Data Encryption:
- Data in transit protected using TLS/SSL encryption to ensure security. 
- Data at rest encrypted using cloud provider's encryption services to ensure security. 

#### Assumptions:
- Cloud-native services are utilized for scalability and reliability.
- Engineers are responsible for managing the Kafka stream.
- Data encryption and access control measures are implemented based on cloud provider best practices.
