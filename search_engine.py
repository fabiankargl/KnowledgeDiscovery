from __future__ import annotations

import argparse
import json
import math
import pickle
import shlex
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Mapping, MutableMapping, Optional, Sequence, Tuple

from build_index import (
    FIELD_BOOSTS,
    FIELDS_KEYWORD,
    FIELDS_NUMERIC,
    FIELDS_TO_INDEX,
    simple_tokenize,
    tf_weight,
)

BASE_DIR = Path(__file__).resolve().parent
DEFAULT_INDEX_DIR = BASE_DIR / "indexes"
DEFAULT_SYNONYM_PATH = BASE_DIR / "dataCleaning" / "synonyms" / "synonymsForSearch.json"


@dataclass
class QueryComponents:
    """Container for parsed query parts."""

    text_terms: MutableMapping[str, Counter]
    required_terms: MutableMapping[str, List[List[str]]]
    numeric_filters: List[Tuple[str, str, float]]
    keyword_filters: MutableMapping[str, List[str]]


@dataclass
class SearchResult:
    """Represents a single search hit."""

    doc_id: int
    tf_idf_score: float
    cosine_score: float


def load_pickle(path: Path):
    try:
        with path.open("rb") as f:
            return pickle.load(f)
    except FileNotFoundError as exc:  # pragma: no cover - defensive programming
        raise FileNotFoundError(
            f"Missing required data file: {path}. Run 'python build_index.py' first."
        ) from exc


def load_synonyms(path: Path) -> Tuple[Dict[str, str], Dict[str, List[str]]]:
    """Load synonym configuration.

    Returns
    -------
    tuple(dict, dict)
        ``(field_aliases, token_synonyms)``.
    """

    if not path.exists():  # pragma: no cover - configuration fallback
        default_aliases: Dict[str, str] = {}
        for field in list(FIELDS_TO_INDEX) + list(FIELDS_NUMERIC) + list(FIELDS_KEYWORD):
            default_aliases[field] = field
            default_aliases[field.replace(" ", "_")] = field
            default_aliases[field.replace(" ", "")] = field
        return default_aliases, defaultdict(list)

    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)

    category_synonyms = data.get("category_synonyms", {})

    # Map aliases to canonical dataset field names.
    field_aliases: Dict[str, str] = {}
    for canonical, aliases in category_synonyms.items():
        canonical_norm = canonical.lower().strip()
        original_canonical = canonical_norm

        # The dataset stores the playing position under "position clean".
        if canonical_norm == "position":
            canonical_norm = "position clean"

        # Allow the original key as an alias as well (e.g. "position").
        field_aliases[original_canonical.replace(" ", "_")] = canonical_norm

        for variant in {
            canonical_norm,
            canonical_norm.replace(" ", "_"),
            canonical_norm.replace(" ", ""),
        }:
            field_aliases[variant] = canonical_norm

        for alias in aliases:
            alias_norm = alias.lower().strip()
            for variant in {
                alias_norm,
                alias_norm.replace(" ", "_"),
                alias_norm.replace(" ", ""),
            }:
                field_aliases[variant] = canonical_norm

    # Build a symmetric synonym map for tokens.
    token_synonyms: Dict[str, List[str]] = defaultdict(list)

    def add_pair(a: str, b: str) -> None:
        a_norm = a.lower().strip()
        b_norm = b.lower().strip()
        if b_norm not in token_synonyms[a_norm]:
            token_synonyms[a_norm].append(b_norm)
        if a_norm not in token_synonyms[b_norm]:
            token_synonyms[b_norm].append(a_norm)

    def extend_from_section(section: Mapping[str, Sequence[str] | str]) -> None:
        for key, value in section.items():
            values = value if isinstance(value, list) else [value]
            for v in values:
                add_pair(key, v)

    extend_from_section(data.get("position_synonyms", {}))
    extend_from_section(data.get("shooting_hand_synonyms", {}))
    extend_from_section(data.get("country_synonyms", {}))
    extend_from_section(data.get("transaction_terms", {}))

    return field_aliases, token_synonyms


class SearchEngine:
    def __init__(
        self,
        index_path: Optional[Path] = None,
        idf_path: Optional[Path] = None,
        norms_path: Optional[Path] = None,
        meta_path: Optional[Path] = None,
        synonyms_path: Optional[Path] = None,
    ) -> None:
        index_path = Path(index_path) if index_path else DEFAULT_INDEX_DIR / "index.pkl"
        idf_path = Path(idf_path) if idf_path else DEFAULT_INDEX_DIR / "idf.pkl"
        norms_path = Path(norms_path) if norms_path else DEFAULT_INDEX_DIR / "doc_norms.pkl"
        meta_path = Path(meta_path) if meta_path else DEFAULT_INDEX_DIR / "doc_meta.pkl"
        synonyms_path = Path(synonyms_path) if synonyms_path else DEFAULT_SYNONYM_PATH

        self.index = load_pickle(index_path)
        self.idf = load_pickle(idf_path)
        self.doc_norms = load_pickle(norms_path)
        self.doc_meta = load_pickle(meta_path)
        self.field_aliases, self.term_synonyms = load_synonyms(synonyms_path)

        self.numeric_max = {
            field: max(values.values()) if values else 0.0
            for field, values in self.index["numeric"].items()
        }

    # ------------------------------------------------------------------
    # Query parsing
    # ------------------------------------------------------------------
    def parse_query(self, query: str) -> QueryComponents:
        tokens = shlex.split(query)

        text_terms: MutableMapping[str, Counter] = defaultdict(Counter)
        required_terms: MutableMapping[str, List[List[str]]] = defaultdict(list)
        numeric_filters: List[Tuple[str, str, float]] = []
        keyword_filters: MutableMapping[str, List[str]] = defaultdict(list)

        general_terms: List[str] = []

        for token in tokens:
            if ":" in token:
                field_part, value_part = token.split(":", 1)
                canonical_field = self.normalise_field(field_part)

                if canonical_field in FIELDS_NUMERIC:
                    comparator, numeric_value = self._parse_numeric_filter(value_part)
                    numeric_filters.append((canonical_field, comparator, numeric_value))
                    continue

                if canonical_field in FIELDS_KEYWORD:
                    keyword_filters[canonical_field].append(value_part.strip("\" ").lower())
                    continue

                if canonical_field in FIELDS_TO_INDEX:
                    values = self._expand_terms(value_part)
                    text_terms[canonical_field].update(values)
                    required_terms[canonical_field].extend(self._filter_alternatives(value_part))
                    continue

                # Unknown field alias; fall back to general term handling.
                general_terms.append(token)
            else:
                general_terms.append(token)

        if general_terms:
            expanded = self._expand_terms(" ".join(general_terms))
            for term in expanded:
                for field in FIELDS_TO_INDEX:
                    text_terms[field][term] += 1

        return QueryComponents(
            text_terms=text_terms,
            required_terms=required_terms,
            numeric_filters=numeric_filters,
            keyword_filters=keyword_filters,
        )

    # ------------------------------------------------------------------
    # Public search API
    # ------------------------------------------------------------------
    def search(
        self,
        query: str,
        top_k: int = 10,
        boost_field: Optional[str] = None,
        boost_strength: float = 0.0,
    ) -> List[SearchResult]:
        components = self.parse_query(query)

        scores: MutableMapping[int, float] = defaultdict(float)
        query_norm_sq = 0.0

        for field, term_counts in components.text_terms.items():
            boost = FIELD_BOOSTS.get(field, 1.0)
            for term, count in term_counts.items():
                postings = self.index["text"][field].get(term)
                idf_value = self.idf[field].get(term)
                if not postings or not idf_value:
                    continue

                query_tf = tf_weight(count)
                query_weight = query_tf * idf_value * boost
                query_norm_sq += query_weight * query_weight

                for doc_id, doc_count in postings.items():
                    doc_tf = tf_weight(doc_count)
                    doc_weight = doc_tf * idf_value * boost
                    scores[doc_id] += query_weight * doc_weight

        if not scores:
            return []

        query_norm = math.sqrt(query_norm_sq) if query_norm_sq else 0.0
        if query_norm == 0.0:
            return []

        results: List[SearchResult] = []
        for doc_id, score in scores.items():
            if not self._passes_filters(doc_id, components):
                continue

            doc_norm = self.doc_norms.get(doc_id)
            if not doc_norm:
                continue

            final_score = score / (doc_norm * query_norm)

            if boost_field:
                final_score = self._apply_boost(doc_id, boost_field, final_score, boost_strength)

            results.append(SearchResult(doc_id=doc_id, tf_idf_score=score, cosine_score=final_score))

        results.sort(key=lambda x: x.cosine_score, reverse=True)
        return results[:top_k]

    # ------------------------------------------------------------------
    # Helper methods
    # ------------------------------------------------------------------
    def normalise_field(self, field: str) -> str:
        field_norm = field.lower().strip().replace(" ", "_")
        if field_norm in self.field_aliases:
            return self.field_aliases[field_norm]

        # Allow direct usage of canonical names as defined in the dataset.
        field_space = field_norm.replace("_", " ")
        if field_space in FIELDS_TO_INDEX:
            return field_space
        if field_space in FIELDS_NUMERIC:
            return field_space
        if field_space in FIELDS_KEYWORD:
            return field_space

        return field_space

    def _expand_terms(self, raw: str) -> List[str]:
        expanded: List[str] = []
        for token in simple_tokenize(raw):
            expanded.append(token)
            for synonym in self.term_synonyms.get(token, []):
                expanded.extend(simple_tokenize(synonym))
        return expanded

    def _filter_alternatives(self, raw: str) -> List[List[str]]:
        groups: List[List[str]] = []
        for token in simple_tokenize(raw):
            options = {token}
            for synonym in self.term_synonyms.get(token, []):
                tokenised = simple_tokenize(synonym)
                if tokenised:
                    options.update(tokenised)
                else:
                    options.add(synonym)
            groups.append(sorted(options))
        return groups

    def _parse_numeric_filter(self, value: str) -> Tuple[str, float]:
        value = value.strip()
        comparator = "="
        for prefix in (">=", "<=", ">", "<", "="):
            if value.startswith(prefix):
                comparator = prefix
                value = value[len(prefix) :]
                break
        try:
            numeric_value = float(value)
        except ValueError:
            raise ValueError(f"Cannot parse numeric filter value: {value!r}") from None
        return comparator, numeric_value

    def _passes_filters(self, doc_id: int, components: QueryComponents) -> bool:
        # Numeric filters
        for field, comparator, value in components.numeric_filters:
            doc_value = self.index["numeric"].get(field, {}).get(doc_id)
            if doc_value is None:
                return False
            if comparator == ">=" and not (doc_value >= value):
                return False
            if comparator == "<=" and not (doc_value <= value):
                return False
            if comparator == ">" and not (doc_value > value):
                return False
            if comparator == "<" and not (doc_value < value):
                return False
            if comparator == "=" and not math.isclose(doc_value, value, rel_tol=1e-4):
                return False

        # Keyword filters
        for field, values in components.keyword_filters.items():
            actual_value = str(self.doc_meta[doc_id].get(field, "")).strip().lower()
            if actual_value not in values:
                return False

        # Required text terms (field filters)
        for field, required_groups in components.required_terms.items():
            postings_for_field = self.index["text"].get(field, {})
            for group in required_groups:
                if not any(doc_id in postings_for_field.get(term, {}) for term in group):
                    return False

        return True

    def _apply_boost(
        self,
        doc_id: int,
        field: str,
        score: float,
        strength: float,
    ) -> float:
        canonical_field = self.normalise_field(field)
        if canonical_field not in self.index["numeric"]:
            return score

        max_value = self.numeric_max.get(canonical_field, 0.0)
        if max_value <= 0:
            return score

        doc_value = self.index["numeric"][canonical_field].get(doc_id, 0.0)
        boost_factor = 1.0 + strength * (doc_value / max_value)
        return score * boost_factor

    # ------------------------------------------------------------------
    # Presentation helpers
    # ------------------------------------------------------------------
    def format_result(self, result: SearchResult) -> str:
        meta = self.doc_meta.get(result.doc_id, {})
        lines = [
            f"TF-IDF dot product: {result.tf_idf_score:.4f}",
            f"Cosine similarity: {result.cosine_score:.4f}",
            f"Name: {meta.get('player name', 'N/A').title()}",
            f"Position: {meta.get('position clean', 'N/A')}",
            f"College: {meta.get('college', 'N/A')}",
            f"Birth city: {meta.get('birth city', 'N/A')} ({meta.get('birth country', 'N/A')})",
            f"Age: {meta.get('age', 'N/A')} | Weight: {meta.get('weight', 'N/A')} kg",
            f"Draft: {meta.get('draft', 'N/A')}",
        ]

        transactions = meta.get("transactions list")
        if transactions:
            lines.append(f"Transactions: {transactions[:120]}...")

        lines.append(f"Profile: {meta.get('profile url', 'N/A')}")
        return "\n".join(lines)


def interactive_loop(engine: SearchEngine, args: argparse.Namespace) -> None:
    print("Enter a query (empty input to exit). Examples:")
    print("  spanish forwards age:>30")
    print('  position:pg birth_country:us age:<25')
    print("  draft:warriors weight:<95")
    print()

    while True:
        try:
            query = input("Query> ").strip()
        except EOFError:
            break

        if not query:
            break

        results = engine.search(
            query,
            top_k=args.top_k,
            boost_field=args.boost_field,
            boost_strength=args.boost_strength,
        )

        if not results:
            print("No matches found.\n")
            continue

        for rank, result in enumerate(results, start=1):
            print(f"#{rank}")
            print(engine.format_result(result))
            print("-" * 60)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Local search engine for athlete data")
    parser.add_argument("--query", "-q", help="Query string. If omitted, launches interactive mode.")
    parser.add_argument("--top-k", type=int, default=5, help="Number of results to display.")
    parser.add_argument(
        "--boost-field",
        help="Optional numeric field used for popularity boosting (e.g., weight).",
    )
    parser.add_argument(
        "--boost-strength",
        type=float,
        default=0.15,
        help="Boost multiplier (0 disables boosting).",
    )
    parser.add_argument(
        "--index-dir",
        type=Path,
        default=DEFAULT_INDEX_DIR,
        help="Directory containing index.pkl, idf.pkl, doc_norms.pkl and doc_meta.pkl.",
    )
    parser.add_argument(
        "--synonyms",
        type=Path,
        default=DEFAULT_SYNONYM_PATH,
        help="Path to the synonymsForSearch.json configuration file.",
    )

    args = parser.parse_args(argv)

    engine = SearchEngine(
        index_path=args.index_dir / "index.pkl",
        idf_path=args.index_dir / "idf.pkl",
        norms_path=args.index_dir / "doc_norms.pkl",
        meta_path=args.index_dir / "doc_meta.pkl",
        synonyms_path=args.synonyms,
    )

    if args.query:
        results = engine.search(
            args.query,
            top_k=args.top_k,
            boost_field=args.boost_field,
            boost_strength=args.boost_strength,
        )
        if not results:
            print("No matches found.")
            return 0

        for rank, result in enumerate(results, start=1):
            print(f"#{rank}")
            print(engine.format_result(result))
            print("-" * 60)
        return 0

    interactive_loop(engine, args)
    return 0


if __name__ == "__main__":
    sys.exit(main())