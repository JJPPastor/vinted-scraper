import pandas as pd
import torch
from transformers import AutoTokenizer, AutoModelForCausalLM, BitsAndBytesConfig
import logging
import gc

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class LocalClothingConditionClassifier:
    def __init__(self, model_name: str = "mistralai/Mistral-7B-Instruct-v0.3", use_4bit: bool = True):
        self.model_name = model_name
        self.use_4bit = use_4bit
        
        self.conditions = ['état neuf', 'excellent état', 'très bon état', 'bon état']
        
        self.system_prompt = """Tu es un expert strict en évaluation de vêtements d'occasion pour plateformes de seconde main. 
Tu dois classifier l'état d'un vêtement basé sur sa description en utilisant EXACTEMENT une de ces catégories:
- état neuf
- excellent état  
- très bon état
- bon état

Critères STRICTS (sois exigeant):
- état neuf: UNIQUEMENT si jamais porté ET étiquettes présentes ET explicitement mentionné "neuf"
- excellent état: porté 1-2 fois maximum, aucun défaut, état quasi parfait
- très bon état: bien entretenu, signes d'usage très légers, bon état général
- bon état: usage normal visible, défauts mineurs, traces d'usure

IMPORTANT: La plupart des articles d'occasion sont en "très bon état". Sois strict pour "état neuf".

Réponds UNIQUEMENT par l'une des quatre catégories exactes."""

        self.model = None
        self.tokenizer = None
        self._load_model()

    def _load_model(self):
        try:
            logger.info(f"Loading model: {self.model_name}")
            
            self.tokenizer = AutoTokenizer.from_pretrained(self.model_name)
            if self.tokenizer.pad_token is None:
                self.tokenizer.pad_token = self.tokenizer.eos_token
            
            if self.use_4bit:
                quantization_config = BitsAndBytesConfig(
                    load_in_4bit=True,
                    bnb_4bit_compute_dtype=torch.float16,
                    bnb_4bit_use_double_quant=True,
                    bnb_4bit_quant_type="nf4"
                )
                
                self.model = AutoModelForCausalLM.from_pretrained(
                    self.model_name,
                    quantization_config=quantization_config,
                    device_map="auto",
                    trust_remote_code=True,
                    torch_dtype=torch.float16
                )
            else:
                self.model = AutoModelForCausalLM.from_pretrained(
                    self.model_name,
                    device_map="auto",
                    trust_remote_code=True,
                    torch_dtype=torch.float16
                )
            
            logger.info("Model loaded successfully!")
            
        except Exception as e:
            logger.error(f"Failed to load model: {e}")
            self.model = None
            self.tokenizer = None

    def _format_prompt(self, description: str) -> str:
        return f"<s>[INST] {self.system_prompt}\n\nDescription: {description} [/INST]"

    def _extract_condition(self, response: str) -> str:
        response_lower = response.lower().strip()
        
        # First check for exact condition matches
        for condition in self.conditions:
            if condition in response_lower:
                return condition
        
        # Apply stricter extraction logic
        if ('neuf' in response_lower and 
            ('jamais' in response_lower or 'étiquette' in response_lower)):
            return 'état neuf'
        elif any(word in response_lower for word in ['excellent', 'parfait', 'impeccable']):
            return 'excellent état'
        elif any(word in response_lower for word in ['très bon', 'très bien']):
            return 'très bon état'
        else:
            return 'très bon état'  # Default to most common

    def _generate_with_model(self, description: str) -> str:
        if self.model is None or self.tokenizer is None:
            return self._rule_based_classification(description)
        
        try:
            prompt = self._format_prompt(description)
            
            inputs = self.tokenizer(prompt, return_tensors="pt", truncation=True, max_length=512)
            inputs = {k: v.to(self.model.device) for k, v in inputs.items()}
            
            with torch.no_grad():
                outputs = self.model.generate(
                    **inputs,
                    max_new_tokens=50,
                    temperature=0.1,
                    do_sample=True,
                    pad_token_id=self.tokenizer.eos_token_id,
                    eos_token_id=self.tokenizer.eos_token_id
                )
            
            response = self.tokenizer.decode(outputs[0], skip_special_tokens=True)
            
            if "[/INST]" in response:
                response = response.split("[/INST]")[-1].strip()
            
            condition = self._extract_condition(response)
            return condition
            
        except Exception as e:
            logger.error(f"Model generation failed: {e}")
            return self._rule_based_classification(description)

    def _rule_based_classification(self, description: str) -> str:
        if pd.isna(description):
            return 'très bon état'
            
        desc_lower = description.lower()
        
        # Very strict criteria for "état neuf" - must have multiple indicators
        neuf_strict = ['neuf', 'étiquette'] 
        jamais_porte = any(phrase in desc_lower for phrase in ['jamais porté', 'jamais utilisé', 'jamais mis'])
        avec_etiquette = any(phrase in desc_lower for phrase in ['avec étiquette', 'étiquettes', 'tags'])
        mot_neuf = 'neuf' in desc_lower
        
        # Count strong "neuf" indicators
        neuf_score = sum([jamais_porte, avec_etiquette, mot_neuf])
        
        # Strong wear indicators
        usure_forte = ['usé', 'usure', 'traces d\'usure', 'défaut', 'abîmé', 'tache', 'troué', 'semelles à refaire', 'légères traces']
        
        # Moderate wear indicators  
        usure_legere = ['porté', 'bien entretenu', 'bon état général', 'quelques fois', 'peu porté']
        
        # Excellent condition indicators
        excellent_keywords = ['parfait état', 'impeccable', 'excellent état', 'quasi parfait']
        
        # Check for explicit condition mentions
        if 'très bon état' in desc_lower:
            return 'très bon état'
        elif 'excellent état' in desc_lower:
            return 'excellent état'
        elif 'bon état' in desc_lower and any(word in desc_lower for word in usure_forte):
            return 'bon état'
        
        # Apply strict scoring
        if neuf_score >= 2:  # Need at least 2 strong indicators for "neuf" 
            return 'état neuf'
        elif any(keyword in desc_lower for keyword in excellent_keywords):
            return 'excellent état'
        elif any(keyword in desc_lower for keyword in usure_forte):
            return 'bon état'
        elif any(keyword in desc_lower for keyword in usure_legere):
            return 'très bon état'
        else:
            return 'très bon état'  # Default to most common category

    def classify_condition(self, description: str) -> str:
        if pd.isna(description) or not str(description).strip():
            return 'bon état'
            
        if self.model is not None:
            return self._generate_with_model(description)
        else:
            return self._rule_based_classification(description)

    def classify_dataframe(self, df: pd.DataFrame, description_column: str = 'description') -> pd.DataFrame:
        if description_column not in df.columns:
            raise ValueError(f"Column '{description_column}' not found in DataFrame")
        
        df_copy = df.copy()
        conditions = []
        
        logger.info(f"Classifying {len(df)} clothing items...")
        
        for idx, description in enumerate(df_copy[description_column]):
            try:
                condition = self.classify_condition(description)
                conditions.append(condition)
                
                if idx % 10 == 0:
                    logger.info(f"Processed {idx}/{len(df)} items")
                    
            except Exception as e:
                logger.error(f"Error processing item {idx}: {e}")
                conditions.append('bon état')
        
        df_copy['condition'] = conditions
        logger.info("Classification completed!")
        
        return df_copy

    def cleanup(self):
        if self.model is not None:
            del self.model
        if self.tokenizer is not None:
            del self.tokenizer
        gc.collect()
        torch.cuda.empty_cache() if torch.cuda.is_available() else None


def classify_clothing_conditions(df: pd.DataFrame, 
                               description_column: str = 'description',
                               model_name: str = "mistralai/Mistral-7B-Instruct-v0.3",
                               use_4bit: bool = True) -> pd.DataFrame:
    classifier = LocalClothingConditionClassifier(model_name=model_name, use_4bit=use_4bit)
    result = classifier.classify_dataframe(df, description_column)
    classifier.cleanup()
    return result

if __name__ == '__main__':
    df = pd.read_csv('../data/balzac-paris_full_vc.csv')
    result_df = classify_clothing_conditions(df)
    print(result_df)
    result_df.to_csv('../data/balzac-paris_full_vc_condition.csv')
