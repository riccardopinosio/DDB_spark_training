# spark_training

This repository contains the material for the spark training sessions, in particular:

1. The slides (under *presentations*)
2. The databricks notebooks

The slides are based on doing things on Azure, but unfortunately it turns out that azure does not support the creation of databricks clusters on their azure for students or free accounts, because of some arcane quota limitations. Therefore, to try out spark, we can use azure databricks community edition. This will allow you to create a small cluster to try out spark. You can sign up to the community edition, and create a spark cluster, here: https://databricks.com/try-databricks?utm_medium=cpc&utm_source=google&utm_campaign=926437904&utm_offer=try-databricks&utm_content=trial&utm_term=databricks%20community%20edition&gclid=EAIaIQobChMIwZOLh8bj7AIVR-N3Ch0AcwDkEAAYASAAEgJ82fD_BwE

In particular, the IoT_databricks_data_analysis notebook will run without problems on the cluster. Similarly, the cluster will also support the analysis of the funda dataset without problems.
The smart meter london dataset is larger (around 10GB), and is pushing the limits of the cluster. Therefore, for that dataset it will be important to aggregate it at a higher level of granularity (from half-hourly to daily) to perform your analysis.

<!---
For the spark trainings, it is important to have an active subscription with azure, that you will use to create resources/load data into resources/process the data. If you don't already have an azure subscription that you want to use, you should create one using one of these steps:

1. Create a free azure account using your HVA email. See https://azure.microsoft.com/en-us/free/
2. In case you cannot do the above (because you already have an azure account on your hva email for some reason), then create a disposable email (e.g. an hotmail email, see https://outlook.live.com/) and use that to register on azure at https://azure.microsoft.com/en-us/free/

**UPDATE: do not use the azure for students subscription (see https://azure.microsoft.com/en-us/free/students/). Unfortunately, it turns out this subscription does not allow you to create databricks clusters because the quota limits for it are too low.** So use the free account (with the HVA email or with a disposable email) instead.

Once you create your azure subscription, you should visit https://portal.azure.com and login with your email and the password you chose when you created the account. After you login, you should get a welcome message and after that, you will se the main screen of the portal, which will look something like this:


![Alt text](https://github.com/riccardopinosio/spark_training/blob/master/assets/Screenshot%202020-10-23%20125638.png?sanitize=true)

On the upper left of the screen, you can use the hamburger button to open the navigation tab. If you click on 'cost management and billing', you will see the details of your subscription. If you use method 1 above to create an azure account, it will say 'azure for students' under subscriptions once you click on the billing tab:

![Alt text](https://github.com/riccardopinosio/spark_training/blob/master/assets/Screenshot%202020-10-23%20130149.png?sanitize=true)

Once you do this, you are good to go for the spark sessions.
-->

Please contact rpinosi2@hva.nl for information regarding this repository.
