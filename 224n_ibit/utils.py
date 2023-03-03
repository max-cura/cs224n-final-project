from datasets import load_dataset

def prepare_soq_dataset_from_file(
        dataset_paths,
        tokenizer,
        generation_prefix = '',
        tokenizer_max_length = None,
):
    raw = load_dataset("csv", data_files=dataset_paths, sep="\t", on_bad_lines='skip')
    filtered = raw.filter(lambda x: x["input"] is not None and x["output"] is not None)

    def soq_tokenize(examples):
        def soq_clean_text(text):
            return text.replace("&#xD;&#xA;", "\n")

        inputs = [generation_prefix + soq_clean_text(text) for text in examples["input"]]
        outputs = [soq_clean_text(text) for text in examples["output"]]
        model_inputs = tokenizer(
            # this is inputs, so `text`, not `target_text`
            text=inputs,
            max_length=tokenizer_max_length,
            padding="max_length",
            truncation=True,
            return_tensors="pt")
        model_inputs["output"] = outputs
        return model_inputs

    dataset = filtered.map(soq_tokenize, batched=True)
    return dataset

def batched_range_iter(start, end, batch_size):
    batch_start = start
    while batch_start < end:
        yield batch_start, min(batch_start+batch_size, end)
        batch_start += batch_size

