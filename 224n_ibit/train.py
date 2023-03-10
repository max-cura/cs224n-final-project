import os

import numpy as np
import torch
from torch.utils.data.dataloader import DataLoader
from transformers import Seq2SeqTrainer, Seq2SeqTrainingArguments, AutoTokenizer, DataCollatorForSeq2Seq, \
    AutoModelForSeq2SeqLM
from datasets import load_dataset
from argparse import ArgumentParser
import evaluate

torch.backends.cuda.matmul.allow_tf32 = True

argp = ArgumentParser()
argp.add_argument("--use-tokenizer", required=True)
argp.add_argument("--dataset-type", required=True)
argp.add_argument("--use-train-dataset", required=True)
argp.add_argument("--use-test-dataset", required=True)
# argp.add_argument("--split-test-amount", type=float, default=0.1)
argp.add_argument("--use-model", required=True)
argp.add_argument("--model-dir", required=True)
argp.add_argument("--metric", default="sacrebleu")
argp.add_argument("--train-batch-size", type=int, default=8)
argp.add_argument("--max-length", type=int, default=2048)
argp.add_argument("--use-generation-prefix", default="soq: ")
# argp.add_argument("--trained-model-name", required=True)
args = argp.parse_args()

# DEVICE

device = torch.cuda.current_device() if torch.cuda.is_available() else 'cpu'

# TOKENIZER

tokenizer = AutoTokenizer.from_pretrained(
    args.use_tokenizer,
    model_max_length=args.max_length,
    use_fast=True,
)

# DATASET

if not os.path.exists(args.use_train_dataset):
    raise RuntimeError(f"Failed to load USE_TRAIN_DATASET <{args.use_train_dataset}>: no such file exists")
if not os.path.exists(args.use_test_dataset):
    raise RuntimeError(f"Failed to load USE_TEST_DATASET <{args.use_test_dataset}>: no such file exists")
print()
print('='*60)
print(f'LOADING AND FILTERING')
print(f'\tUSE_TRAINING_DATASET={args.use_train_dataset}')
print(f'\tUSE_TEST_DATASET={args.use_test_dataset}')
dataset = load_dataset(
    'csv',
    data_files={
        'train': args.use_train_dataset,
        'test': args.use_test_dataset,
    },
    sep="\t",
    on_bad_lines='skip',
)
print(dataset)
if dataset["train"].num_columns != 2:
    raise RuntimeError(f"Failed to load USE_TRAIN_DATASET <{args.use_train_dataset}>: wrong number of columns: expected 2 got {dataset['train'].num_columns}")
if dataset["test"].num_columns != 2:
    raise RuntimeError(f"Failed to load USE_TEST_DATASET <{args.use_test_dataset}>: wrong number of columns: expected 2 got {dataset['test'].num_columns}")
if 'input' not in dataset["train"].column_names or 'output' not in dataset["train"].column_names:
    raise RuntimeError(f"Failed to load USE_TRAIN_DATASET <{args.use_train_dataset}>: wrong column names: expected 'input' and 'output', got {dataset.column_names.keys()}")
if 'input' not in dataset["test"].column_names or 'output' not in dataset["test"].column_names:
    raise RuntimeError(f"Failed to load USE_TEST_DATASET <{args.use_test_datset}>: wrong column names: expected 'input' and 'output', got {dataset.column_names.keys()}")

dataset = dataset.filter(lambda x: x["input"] is not None and x["output"] is not None)

def ds_tokenize(tokenizer, max_length, text=None, text_target=None):
    return tokenizer(
        text=text,
        text_target=text_target,
        # padding="max_length",
        truncation=True,
        # return_tensors="pt"
        max_length=max_length,
    )
def ds_local_tokenize_for_training(examples):
    inputs = [args.use_generation_prefix + text for text in examples["input"]]
    model_inputs = ds_tokenize(tokenizer, text=inputs, max_length=args.max_length)
    labels = ds_tokenize(tokenizer, text_target=examples["output"], max_length=args.max_length)
    model_inputs["labels"] = labels.input_ids
    return model_inputs

def ds_soq_tokenize_for_training(examples):
    def soq_clean_text(text):
        return text.replace("&#xD;&#xA;", "\n")
    inputs = [args.use_generation_prefix + soq_clean_text(text) for text in examples["input"]]
    outputs = [soq_clean_text(text) for text in examples["output"]]
    model_inputs = ds_tokenize(tokenizer, text=inputs, max_length=args.max_length)
    labels = ds_tokenize(tokenizer, text_target=outputs, max_length=args.max_length)
    model_inputs["labels"] = labels['input_ids']
    return model_inputs

print("")
print("="*60)
print("TOKENIZING")

if args.dataset_type == 'local':
    dataset = dataset.map(ds_local_tokenize_for_training, batched=True)
elif args.dataset_type == 'soq':
    dataset = dataset.map(ds_soq_tokenize_for_training, batched=True)
else:
    raise RuntimeError(f"Unknown DATASET_TYPE: {args.dataset_type}")

trainer_args = Seq2SeqTrainingArguments(
    output_dir=args.model_dir,
    evaluation_strategy="epoch",
    eval_steps=10,
    logging_strategy="epoch",
    save_strategy="epoch",
    # since this defaults to AdamW
    #learning_rate=(3e-4 if 't5' in args.use_model else 4e-5),
    learning_rate=4e-5,
    per_device_train_batch_size=args.train_batch_size,
    per_device_eval_batch_size=args.train_batch_size,
    weight_decay=0.01,
    save_total_limit=3,
    num_train_epochs=10,
    predict_with_generate=True,
    load_best_model_at_end=True,
    metric_for_best_model=args.metric,
    # report_to=["tensorboard"],
    #gradient_accumulation_steps=4,
    gradient_checkpointing=True,
    #use_cache=False,
    optim="adafactor",
    dataloader_num_workers=8,
)

data_collator = DataCollatorForSeq2Seq(tokenizer)
metric = evaluate.load(args.metric)

def compute_metrics(eval_pred):
    logits, labels = eval_pred
    predictions = np.argmax(logits, axis=-1)
    labels = np.where(labels != -100, labels, tokenizer.pad_token_id)
    labels = np.argmax(labels, axis=-1)
    #print("\n\n\nPREDICTIONS\n")
    #print(predictions)
    #print("\n\n\nREFERENCES\n")
    #print(labels)
    predictions = tokenizer.batch_decode(predictions, skip_special_tokens=True)
    references = tokenizer.batch_decode(labels, skip_special_tokens=True)
    result = metric.compute(predictions=predictions, references=references)
    print(result)
    return result

#class CustomSeq2SeqTrainer(Seq2SeqTrainer):
#    def get_train_dataloader(self):
#        train_dataloader = DataLoader(
#                dataset["train"],
#                batch_size=trainer_args.per_device_train_batch_size,
#                pin_memory=True,
#                num_workers=8
#        )
#        return train_dataloader

trainer = Seq2SeqTrainer(
    model_init=lambda: AutoModelForSeq2SeqLM.from_pretrained(args.use_model),
    args=trainer_args,
    train_dataset=dataset['train'],
    eval_dataset=dataset['test'],
    data_collator=data_collator,
    tokenizer=tokenizer,

    compute_metrics=compute_metrics,
    # optimizers=(torch.optim.AdamW, None)
)

print("")
print("=" * 60)
print("TRAINING")

trainer.train()
