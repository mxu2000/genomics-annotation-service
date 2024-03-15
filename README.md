# gas-framework
An enhanced web framework (based on [Flask](http://flask.pocoo.org/)) for use in the capstone project. Adds robust user authentication (via [Globus Auth](https://docs.globus.org/api/auth)), modular templates, and some simple styling based on [Bootstrap](http://getbootstrap.com/).

Directory contents are as follows:
* `/web` - The GAS web app files
* `/ann` - Annotator files
* `/util` - Utility scripts for notifications, archival, and restoration
* `/aws` - AWS user data files

# Note to graders:

I only completed exercises up to exercise 10 due to issues with my load balancer listed here: https://edstem.org/us/courses/51874/discussion/4519496

After receiving help on Ed of re-running the domain script, I deleted the existing domain and updated to ELB. However, the issue remained.
![image](https://github.com/MPCS-51083-Cloud-Computing/final-project-mxu2000/assets/111541644/dae92e37-a70b-4e27-b83e-6b9813e335ae)

Because of this issue, I deleted my load balancer and am running my web server on my web instance (maxinexu-gas-web). The GAS website is available here: **https://maxinexu.mpcs-cc.com:4433**

# Process explanations:
Exercise 7 (archival process)

For the archival process, I use a message queue that receives notifications and has a delivery delay of 5 minutes to account for the free users’ download limit  (messages are sent to the maxinexu_archive queue during the “run” process for free users and the archive process is started when the message is received). I used this approach as it decouples the archival process from other tasks, ensuring modular maintenance and scalability.

Simplified archival process diagram:

![image](https://github.com/MPCS-51083-Cloud-Computing/final-project-mxu2000/assets/111541644/d21609a9-2c15-4f70-8749-ba1432e0ed93)


Exercise 9 (restoration process)

I use message queues in restore.py (messages are sent when initiating glacier job and are queued in maxinexu_thaw) and thaw.py (receives message from maxinexu_thaw queue and kickstarts thawing process) which allows for asynchronous processing so users don’t have to wait for the restoration process to be done before annotating other files and doing other tasks. Additionally, this method is scalable and makes the system more modular. Thus, when a lot of users upgrade to premium, the system can handle a large number of restoration requests and changes in the restoration process can be made without affecting other parts.

Simplified restoration process diagram:

![image](https://github.com/MPCS-51083-Cloud-Computing/final-project-mxu2000/assets/111541644/a9ded67f-ba4d-4cc1-ae8c-b7044998674a)

