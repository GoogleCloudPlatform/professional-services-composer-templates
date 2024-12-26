# type: ignore

# Define variables
project_id = "composer-templates-dev"
region = "us-central1"
pro_model = "gemini-1.5-pro-002"
system_instruction = "Always respond in plain-text only."
sample_prompt = ["Summarize the following article. Article: To make a classic spaghetti carbonara, start by bringing a large pot of salted water to a boil. While the water is heating up, cook pancetta or guanciale in a skillet with olive oil over medium heat until it's crispy and golden brown. Once the pancetta is done, remove it from the skillet and set it aside. In the same skillet, whisk together eggs, grated Parmesan cheese, and black pepper to make the sauce. When the pasta is cooked al dente, drain it and immediately toss it in the skillet with the egg mixture, adding a splash of the pasta cooking water to create a creamy sauce."]
deterministic_gen_config = { 
    "top_k": 1,
    "top_p": 0.0,
    "temperature": 0.1
}

validate_tokens_op_kwargs = {
    "character_budget":"1000",
    "token_budget":"500",
    "total_billable_characters":"{{ task_instance.xcom_pull(task_ids='count_tokens_task', key='total_billable_characters') }}",
    "total_tokens":"{{ task_instance.xcom_pull(task_ids='count_tokens_task', key='total_tokens') }}"
}


# Define Airflow DAG default_args
default_args = {
    "owner": 'Google',
    "retries": 0,
    "email_on_failure": False,
    "email_on_retry": False,
    "depends_on_past": False,
    "retry_delay": timedelta(minutes=1),
    "sla": timedelta(minutes=55),
    "execution_timeout": timedelta(minutes=60),
    "start_date": '2024-01-01',
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
    "sla": timedelta(minutes=55),
    "execution_timeout": timedelta(minutes=60)
}