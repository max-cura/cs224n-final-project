import argparse
import torch
from datasets import load_dataset
from tqdm import tqdm
from transformers import T5ForConditionalGeneration, GenerationConfig, T5TokenizerFast
import evaluate

import os
import utils

# ARGUMENTS

argp = argparse.ArgumentParser()
argp.add_argument("--use-tokenizer", required=True)
argp.add_argument("--evaluation-dataset", required=True)
argp.add_argument("--model", required=True)
argp.add_argument("--use-generation-prefix", default="soq: ")
argp.add_argument("--max-length", type=int, default=2048)
argp.add_argument("--metric", default="sacrebleu")
argp.add_argument("--batch-size", type=int, default=64)
args = argp.parse_args()

# DEVICE

device = torch.cuda.current_device() if torch.cuda.is_available() else 'cpu'

# TOKENIZER & MODEL CREATION

tokenizer = T5TokenizerFast.from_pretrained(
    args.use_tokenizer,
    model_max_length=args.max_length
)
model = T5ForConditionalGeneration.from_pretrained(args.model).to(device)

if not os.path.isdir(args.model) or not os.path.isfile(args.evaluation_dataset):
    raise ValueError(f"bad path for model or evaluation dataset")
dataset = utils.prepare_soq_dataset_from_file(
    args.evaluation_dataset,
    tokenizer=tokenizer,
    generation_prefix=args.use_generation_prefix,
    tokenizer_max_length=args.max_length,
)
tch_dataset = dataset.with_format("torch")

print("Starting predictions...")

predictions = torch.zeros(tch_dataset['train']['input_ids'].shape, dtype=torch.int)

with torch.no_grad():
    batch_count = (len(tch_dataset['train']) + args.batch_size - 1) // args.batch_size
    for batch_start, batch_end in tqdm(
            utils.batched_range_iter(0, len(tch_dataset['train']), batch_size=args.batch_size),
            total=batch_count):
        # print(f"Batch {batch_start}:{batch_end}")
        batch_inputs = tch_dataset['train']['input_ids'][batch_start:batch_end].to(device)
        batch_attention_mask = tch_dataset['train']['attention_mask'][batch_start:batch_end].to(device)
        batch_predictions = model.generate(
            input_ids=batch_inputs,
            attention_mask=batch_attention_mask,
            do_sample=False,
            generation_config=GenerationConfig(
                max_new_tokens=args.max_length,
                max_length=args.max_length,
            )
        ).to('cpu')
        predictions[batch_start:batch_end, 0:batch_predictions.shape[1]] = batch_predictions[:, :].type(dtype=torch.int)

#print(predictions.shape)
#print(predictions[0])

print("Finished predictions, evaluating...")

metric = evaluate.load(args.metric)

predictions_decoded = tokenizer.batch_decode(predictions, skip_special_tokens=True)

results = metric.compute(
    predictions=predictions_decoded,
    references=dataset['train']['output']
)
print(results)
