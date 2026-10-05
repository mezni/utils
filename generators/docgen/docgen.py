import argparse
import json
import os
import random
import re
import sys
from datetime import UTC, datetime
from pathlib import Path

from dotenv import load_dotenv
from openai import OpenAI, OpenAIError

# Load environment variables
load_dotenv()

# Initialize OpenRouter Client
client = OpenAI(
    base_url="https://openrouter.ai/api/v1",
    api_key=os.getenv("OPENROUTER_API_KEY"),
)

STATE_FILE = Path("state.json")
DATA_ROOT = "data"

# Model used for every structured (JSON) generation call. Override with DOCGEN_MODEL.
# Defaults to a free tier so the script runs without OpenRouter credits.
MODEL = os.getenv("DOCGEN_MODEL", "qwen/qwen3.8-27b:free")

DEFAULT_DEPARTMENTS = ["Operations", "Billing", "Security", "Engineering", "Human Resources", "Legal"]
LLM_ERRORS = (OpenAIError, ValueError, KeyError, TypeError, IndexError)

DOC_FORMATS = ("pdf", "html", "docx", "markdown", "text")
FORMAT_EXTENSIONS = {
    "pdf": ".pdf",
    "html": ".html",
    "docx": ".docx",
    "markdown": ".md",
    "text": ".txt",
}
PRIVACY_LEVELS = ("Public", "Internal", "Confidential", "Restricted")
AUDIENCE_POOL = (
    "All employees",
    "Department heads",
    "Engineering and operations staff",
    "Managers and team leads",
    "External auditors and regulators",
    "Contractors and third-party vendors",
    "Executive leadership",
    "New joiners during onboarding",
)

FOCUS_AREAS = {
    "engineering": (
        "Code Review", "Release Management", "Incident Response", "Change Control",
        "Technical Debt", "Build and Deployment", "On-Call Rotation",
        "Dependency Management", "Testing Standards", "Architecture Review",
    ),
    "product": (
        "Roadmap Planning", "Feature Flagging", "Product Discovery", "Beta Testing",
        "Release Notes", "Customer Feedback", "Pricing Changes",
        "Requirements Traceability", "Product Metrics", "Experimentation",
    ),
    "operations": (
        "Capacity Planning", "Service Continuity", "Vendor Management", "Asset Lifecycle",
        "Backup and Restore", "Facilities Access", "Process Automation",
        "Performance Monitoring", "Cost Governance", "Inventory Accuracy",
    ),
    "legal": (
        "Contract Review", "Regulatory Compliance", "Intellectual Property", "Litigation Hold",
        "Data Privacy", "Licensing", "Corporate Governance", "Risk Assessment",
        "Policy Enforcement", "Records of Authority",
    ),
    "finance": (
        "Expense Management", "Revenue Recognition", "Audit Preparation", "Budget Control",
        "Procurement", "Treasury Operations", "Tax Reporting",
        "Financial Close", "Credit Risk", "Cost Allocation",
    ),
    "hr": (
        "Recruitment", "Performance Review", "Employee Relations", "Compensation",
        "Health and Safety", "Workplace Conduct", "Learning and Development",
        "Remote Work", "Succession Planning", "Data Protection",
    ),
    "sales": (
        "Lead Qualification", "Discount Approval", "Account Management", "Contract Negotiation",
        "Channel Partners", "Sales Forecasting", "Customer Onboarding",
        "Territory Planning", "Renewal Management", "Commission Structure",
    ),
    "marketing": (
        "Brand Management", "Campaign Approval", "Content Publishing", "Social Media",
        "Market Research", "Event Management", "Analyst Relations",
        "Web Accessibility", "Press Response", "Segmentation",
    ),
    "security": (
        "Access Control", "Vulnerability Management", "Encryption Standards", "Security Awareness",
        "Incident Classification", "Third-Party Risk", "Penetration Testing",
        "Logging and Monitoring", "Data Classification", "Secure Disposal",
    ),
    "customer support": (
        "Escalation Handling", "Service Level Targets", "Knowledge Management", "Ticket Triage",
        "Customer Communication", "Warranty Claims", "Feedback Capture",
        "On-Call Coverage", "Refund Handling", "Quality Assurance",
    ),
}

# Used for any department without a bespoke list, so every department can still
# reach the document count it was assigned.
DEFAULT_FOCUS = (
    "Service Delivery", "Quality Management", "Risk Management", "Resource Planning",
    "Reporting", "Performance Monitoring", "Business Continuity", "Data Management",
    "Change Management", "Training and Onboarding", "Vendor Oversight", "Incident Response",
    "Compliance Assurance", "Asset Management", "Knowledge Management", "Process Improvement",
)

# The industry drives the whole document set, so it is configurable rather than
# hardcoded: --industry=<name> wins, then DOCGEN_INDUSTRY, then a random pick.
INDUSTRY_POOL = (
    "Technology", "Healthcare", "Financial Services", "Logistics", "Renewable Energy",
    "Manufacturing", "Retail", "Education", "Media and Entertainment", "Agriculture",
    "Construction", "Telecommunications", "Hospitality", "Insurance", "Pharmaceuticals",
)

NAME_STEMS = (
    "Northwind", "Apex", "Vertex", "Lumen", "Harbour", "Quanta", "Solstice", "Meridian",
    "Cobalt", "Zenith", "Ironwood", "Calder", "Pinnacle", "Aster", "Fairmont", "Granite",
)

COMPANY_SUFFIXES = ("Systems", "Group", "Solutions", "Holdings", "Industries", "Partners")

INDUSTRY_SUFFIXES = {
    "technology": ("Systems", "Labs", "Digital", "Dynamics"),
    "healthcare": ("Health", "Care Group", "Medical", "Clinical"),
    "financial services": ("Capital", "Financial", "Partners", "Trust"),
    "logistics": ("Logistics", "Freight", "Supply", "Distribution"),
    "renewable energy": ("Energy", "Power", "Renewables", "Utilities"),
    "manufacturing": ("Industries", "Manufacturing", "Fabrication", "Works"),
    "retail": ("Retail", "Commerce", "Stores", "Markets"),
    "education": ("Learning", "Education", "Institute", "Academy"),
    "media and entertainment": ("Media", "Studios", "Broadcasting", "Entertainment"),
    "agriculture": ("Agricultural", "Farms", "Agro", "Growers"),
    "construction": ("Construction", "Builders", "Developments", "Contracting"),
    "telecommunications": ("Telecom", "Networks", "Communications", "Connect"),
    "hospitality": ("Hospitality", "Hotels", "Resorts", "Hospitality Group"),
    "insurance": ("Insurance", "Assurances", "Underwriting", "Risk"),
    "pharmaceuticals": ("Pharmaceuticals", "Therapeutics", "Labs", "Bio"),
}


# ==========================================
# Helper Functions
# ==========================================

def slugify(value: str) -> str:
    """Coerces a value into a lowercase word usable as a directory or ID name.
    Spaces are replaced by hyphens, other non-alphanumeric chars are removed.
    """
    value = value.strip().lower()
    value = re.sub(r"[^a-z0-9\s]", "", value)  # remove non-alphanum except spaces
    value = re.sub(r"\s+", "-", value)  # replace spaces with hyphens
    if not value:
        # Stay empty so callers can detect the absence of a usable name and
        # substitute their own fallback rather than silently getting "c".
        return ""
    if not value[0].isalpha():
        value = f"c{value}"
    return value


def normalize_company_code(code: str) -> str:
    """Coerces a model-suggested code into a clean lowercase word."""
    return slugify(code)


def document_id(title: str, extension: str = "") -> str:
    """Builds a document id from its title: lowercase, spaces replaced by underscores.

    The extension defaults to the one already on the title, but can be overridden so
    the id matches the format the file is actually written in, e.g.
    "Data Retention Policy" + ".md" -> "data_retention_policy.md".
    """
    clean = re.sub(r"[^a-z0-9.\s]+", "", title.strip().lower())
    slug = re.sub(r"\s+", "_", clean).strip("_")
    if not extension:
        return slug or "document"
    stem, _, _ = slug.rpartition(".")
    return f"{stem or slug or 'document'}{extension}"


def document_key(title_or_id: str) -> str:
    """Normalises a title or document id to a stable comparison key.

    Unlike document_id this is idempotent, so a key derived from a title
    ("Product Catalog" -> "product_catalog") matches one derived from the id that
    was written to disk ("product_catalog.pdf" -> "product_catalog"). document_id
    cannot be reused for this because it strips underscores and so is not
    self-consistent.
    """
    stem = Path(str(title_or_id)).stem
    return re.sub(r"[^a-z0-9]+", "_", stem.strip().lower()).strip("_") or "document"


def parse_json_response(raw: str) -> dict:
    """Parses a model response that may be wrapped in markdown code fences."""
    raw = raw.strip()
    if raw.startswith("```"):
        raw = raw.split("```")[1]
        if raw.lower().startswith("json"):
            raw = raw[4:]
        raw = raw.strip()
    return json.loads(raw)


def create_company_directories(company_id: str, department_names: list, root: str = DATA_ROOT) -> str:
    """Creates data/<company_id>/<department>/ directories on disk and returns the company path."""
    company_path = os.path.join(root, company_id)
    os.makedirs(company_path, exist_ok=True)
    for department in department_names:
        dept_slug = slugify(department)
        os.makedirs(os.path.join(company_path, dept_slug), exist_ok=True)
    return company_path


def department_directory(company_id: str, dept_id: str, root: str = DATA_ROOT) -> str:
    """Returns data/<company_id>/<dept_id>/, creating it if it does not exist."""
    path = os.path.join(root, company_id, dept_id)
    os.makedirs(path, exist_ok=True)
    return path


def write_document(
    company_id: str,
    dept_id: str,
    filename: str,
    content: str | bytes,
    root: str = DATA_ROOT,
) -> str:
    """Writes content to data/<company_id>/<dept_id>/<filename> and returns the full path.

    Accepts bytes so binary formats (.pdf, .docx) are written through unchanged.
    """
    path = os.path.join(department_directory(company_id, dept_id, root), filename)
    if isinstance(content, bytes):
        with open(path, "wb") as f:
            f.write(content)
    else:
        with open(path, "w", encoding="utf-8") as f:
            f.write(content)
    return path


def fill_to_count(names: list, fallbacks: list, count: int) -> list:
    """Returns up to count unique names, topping up from fallbacks without duplicating.

    If the fallbacks run out the list is simply shorter than count rather than raising.
    """
    merged: list = []
    seen: set[str] = set()
    for candidate in [*names, *fallbacks]:
        if len(merged) >= count:
            break
        name = str(candidate).strip()
        key = name.lower()
        if not name or key in seen:
            continue
        seen.add(key)
        merged.append(name)
    return merged


def normalize_policies(
    raw: object,
    count: int,
    rng: random.Random,
    exclude: set | None = None,
) -> list[dict]:
    """Coerces model-supplied policy entries into full metadata dicts.

    The model supplies the title, version, audience and privacy classification. The
    output format is chosen locally so the five formats stay evenly represented no
    matter what the model returns.
    """
    policies: list[dict] = []
    # Keyed on the id stem so exclusions passed in as document ids match.
    used = {document_key(t) for t in (exclude or set())}
    if not isinstance(raw, list):
        return policies

    for item in raw:
        if not isinstance(item, dict):
            continue
        title = str(item.get("title") or "").strip()
        if not title:
            continue

        # Strip any extension the model may have appended; format drives the suffix.
        title = Path(title).stem.strip()
        if not title or document_key(title) in used:
            continue

        version = str(item.get("version") or "").strip() or "1.0"
        audience = str(item.get("audience") or "").strip() or rng.choice(AUDIENCE_POOL)
        privacy = str(item.get("privacy") or "").strip()
        privacy = next((p for p in PRIVACY_LEVELS if p.lower() == privacy.lower()), None)
        privacy = privacy or rng.choice(PRIVACY_LEVELS)

        doc_format = rng.choice(DOC_FORMATS)
        used.add(document_key(title))
        policies.append(
            {
                "title": title,
                "format": doc_format,
                "id": document_id(title, FORMAT_EXTENSIONS[doc_format]),
                "version": version,
                "audience": audience,
                "privacy": privacy,
                "pages": random.randint(5, 10),
            }
        )
        if len(policies) >= count:
            break
    return policies


def fallback_policies(
    dept_name: str,
    industry: str,
    count: int,
    rng: random.Random,
    exclude: set | None = None,
) -> list[dict]:
    """Builds varied policy metadata without calling the model.

    Titles are drawn from the industry's own document subjects first, then the
    department's focus areas, so a telecom run produces roaming and tariff documents
    rather than generic corporate paperwork. Always returns exactly `count` entries so
    each department keeps the document count it was assigned.
    """
    subjects = list(industry_subjects(industry))
    areas = list(FOCUS_AREAS.get(dept_name.lower(), ()))
    rng.shuffle(subjects)
    rng.shuffle(areas)
    # Industry subjects are already complete document names ("Product Catalog"), so
    # they must not get a suffix bolted on. Focus areas are topics and need one.
    topics = areas + [a for a in DEFAULT_FOCUS if a not in areas]

    suffixes = ("Policy", "Standard", "Procedure", "Guideline", "Protocol", "Charter")
    policies: list[dict] = []
    # Keyed on the id stem, not the title, so exclusions passed in as document ids
    # ("product_catalog.pdf") actually match what is generated here.
    used: set = {document_key(t) for t in (exclude or set())}

    def add(title: str) -> None:
        doc_format = rng.choice(DOC_FORMATS)
        used.add(document_key(title))
        policies.append(
            {
                "title": title,
                "format": doc_format,
                "id": document_id(title, FORMAT_EXTENSIONS[doc_format]),
                "version": f"{rng.randint(1, 3)}.{rng.randint(0, 9)}",
                "audience": rng.choice(AUDIENCE_POOL),
                "privacy": rng.choice(PRIVACY_LEVELS),
                "pages": rng.randint(5, 10),
            }
        )
        used.add(document_key(title))

    for subject in subjects:
        if len(policies) >= count:
            break
        if document_key(subject) not in used:
            add(subject)

    # Same subject, different angle, so a department covering a broad subject still
    # gets several distinct documents rather than repeating one title. Aspects are the
    # outer loop so consecutive documents differ in both subject and angle.
    aspects = list(ASPECT_QUALIFIERS)
    rng.shuffle(aspects)
    for aspect in aspects:
        for subject in subjects:
            if len(policies) >= count:
                break
            title = f"{subject} - {aspect}"
            if document_key(title) not in used:
                add(title)

    for area in topics:
        if len(policies) >= count:
            break
        title = f"{area} {rng.choice(suffixes)}"
        if document_id(title).lower() not in used:
            add(title)

    # Exhaustive top-up, so a short focus list can never leave a department under-filled.
    for area in topics:
        for suffix in suffixes:
            if len(policies) >= count:
                break
            title = f"{area} {suffix}"
            if document_key(title) not in used:
                add(title)

    return policies[:count]


def distinct_counts(total: int, low: int = 5, high: int = 10, rng: random.Random | None = None) -> list[int]:
    """Returns `total` mutually different counts drawn from [low, high].

    With more departments than available values the range is widened rather than
    repeating a count, so the "different number of files" rule still holds.
    """
    rng = rng or random
    if total <= 0:
        return []
    spread = high - low + 1
    if total > spread:
        high = low + total - 1
        spread = total
    return rng.sample(range(low, high + 1), total)


def fallback_identity(industry: str) -> tuple[str, str]:
    """Derives a plausible company name and single-word id without calling the model.

    The name is composed from a distinctive stem plus the industry, never by echoing
    the industry back on its own, so a rate-limited run still yields a real name.
    """
    label = industry.strip() or "Business"
    stem = random.choice(NAME_STEMS)
    label_words = [w.lower().strip(".,&") for w in label.split()]

    def overlaps(word: str) -> bool:
        """True when a word repeats the industry, catching both "Agriculture/Agricultural"
        (shared prefix) and "Telecommunications/Communications" (contained substring)."""
        target = word.lower().strip(".,&")
        if len(target) < 4:
            return False
        for existing in label_words:
            if len(existing) < 5:
                continue
            if len(os.path.commonprefix([existing, target])) >= 5:
                return True
            if len(target) >= 4 and (target in existing or existing in target):
                return True
        return False

    suffix = INDUSTRY_SUFFIXES.get(label.lower(), COMPANY_SUFFIXES)
    available = [s for s in suffix if not overlaps(s.split()[0])]
    if not available:
        available = list(COMPANY_SUFFIXES)
    name = f"{stem} {label} {random.choice(available)}".strip()
    return name, normalize_company_code(stem) or "company"


def resolve_industry(cli_value: str = "") -> str:
    """Resolves the industry from the CLI flag, the environment, or a random pick."""
    if cli_value:
        return cli_value
    env_value = os.getenv("DOCGEN_INDUSTRY", "").strip()
    if env_value:
        return env_value
    return random.choice(INDUSTRY_POOL)


# ==========================================
# LLM Generation Functions
# ==========================================

def send_message(
    prompt: str,
    system_prompt: str = "You are a helpful assistant.",
    model: str = "openrouter/free",
    max_tokens: int = 500,
) -> str:
    """Sends a message to an OpenRouter model and returns the response content."""
    try:
        completion = client.chat.completions.create(
            extra_headers={
                "HTTP-Referer": "https://your-site-url.com",
                "X-Title": "My Python Script",
            },
            model=model,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": prompt},
            ],
            max_tokens=max_tokens,
        )
        return completion.choices[0].message.content
    except OpenAIError as e:
        return f"Error sending message: {e}"


def generate_company_name_and_id(industry: str, max_tokens: int = 200) -> dict[str, str]:
    """Generates a company name and single-word company ID based on an industry domain."""
    prompt = f"""
    Given the industry domain '{industry}':
    1. Generate a short, single-word company id (lowercase alphanumeric only, suitable for use as a directory name).
    2. Generate a plausible company name for a business in this industry.

    Return ONLY a valid JSON object with exact keys "company_name" and "company_id".
    Example format:
    {{
        "company_name": "Apex Technology Solutions",
        "company_id": "apextech"
    }}
    Do not include markdown code fences or explanatory text.
    """

    try:
        response = client.chat.completions.create(
            model=MODEL,
            messages=[{"role": "user", "content": prompt}],
            temperature=0.7,
            max_tokens=max_tokens,
        )
        data = parse_json_response(response.choices[0].message.content)

        company_name = str(data.get("company_name") or "").strip()
        company_id = normalize_company_code(str(data.get("company_id") or ""))

        if not company_id:
            company_id = fallback_identity(industry)[1]
        if not company_name:
            company_name = industry.strip()

        return {"company_name": company_name, "company_id": company_id}
    except LLM_ERRORS as e:
        fallback_name, fallback_id = fallback_identity(industry)
        return {
            "company_name": fallback_name,
            "company_id": fallback_id,
            "warning": f"Falling back to derived identity: {e}",
        }


def generate_departments(
    domain: str,
    company_name: str = "",
    count: int | None = None,
    max_tokens: int = 400,
) -> list:
    """Generates a list of department names for a company.
    If count is None, randomly picks between 3 and 6 departments.
    """
    if count is None:
        count = random.randint(3, 6)

    context = f" for '{company_name}'" if company_name else ""
    prompt = f"""
    Generate {count} core departments{context} in the industry domain '{domain}'.
    Each department should be a short, human-readable title (e.g., Operations, Billing,
    Security, Customer Support). Do not include numbering or punctuation at the ends.

    Return ONLY a valid JSON object with the exact key "departments" holding an array of strings.
    Example format:
    {{
        "departments": ["Operations", "Billing", "Security"]
    }}
    Do not include markdown code fences or explanatory text.
    """

    try:
        response = client.chat.completions.create(
            model=MODEL,
            messages=[{"role": "user", "content": prompt}],
            temperature=0.7,
            max_tokens=max_tokens,
        )
        data = parse_json_response(response.choices[0].message.content)

        raw = data.get("departments") or []
        if isinstance(raw, str):
            raw = [raw]

        departments: list = []
        seen = set()
        for item in raw:
            name = str(item).strip().strip(".,:;\"'")
            if not name:
                continue
            key = name.lower()
            if key in seen:
                continue
            seen.add(key)
            departments.append(name)

        return fill_to_count(departments, DEFAULT_DEPARTMENTS, count)

    except LLM_ERRORS as e:
        print(f"Warning: falling back to default departments: {e}")
        return fill_to_count([], DEFAULT_DEPARTMENTS, count)


def generate_department_policies(
    domain: str,
    dept_name: str,
    company_name: str = "",
    count: int = 5,
    rng: random.Random | None = None,
    exclude: set | None = None,
    max_tokens: int = 900,
) -> list[dict]:
    """Generates full document metadata for a department's policy set.

    The model supplies the names, versions, audience and privacy classification. The
    file format is chosen locally, and the count is supplied by the caller so each
    department can receive a different number of documents.
    """
    rng = rng or random
    context = f" at '{company_name}'" if company_name else ""
    dept_topics = ", ".join(FOCUS_AREAS.get(dept_name.lower(), ("Operations",))[:6])
    industry_topics = ", ".join(industry_subjects(domain)[:10]) or "the core operational processes"
    prompt = f"""
    Invent {count} distinct internal policy documents for the '{dept_name}' department{context}
    in the '{domain}' sector. Typical subject areas for this department
    include: {dept_topics}.

    Documents in this industry typically cover: {industry_topics}.
    Draw titles from that subject matter where it suits the department, so the documents
    read as real {domain} business documents rather than generic corporate paperwork.
    Make every title specific and different from the others.

    Return ONLY a valid JSON object with the key "policies" containing an array of
    {count} objects. Each object must have exactly these keys:
    - "title": a descriptive document title, no file extension, e.g. "Third-Party Risk Assessment Procedure"
    - "version": a version string, e.g. "1.0" or "2.1"
    - "audience": who the document is written for, e.g. "Department heads"
    - "privacy": exactly one of "Public", "Internal", "Confidential", "Restricted"

    Example format:
    {{
        "policies": [
            {{
                "title": "Third-Party Risk Assessment Procedure",
                "version": "1.0",
                "audience": "Department heads",
                "privacy": "Confidential"
            }}
        ]
    }}
    Do not include markdown code fences or explanatory text.
    """

    data: dict = {}
    try:
        response = client.chat.completions.create(
            model=MODEL,
            messages=[{"role": "user", "content": prompt}],
            temperature=0.7,
            max_tokens=max_tokens,
        )
        data = parse_json_response(response.choices[0].message.content)
        policies = normalize_policies(data.get("policies"), count, rng, exclude)
        if len(policies) >= count:
            return policies
        print(f"Warning: model returned {len(policies)}/{count} policies for '{dept_name}'.")

    except LLM_ERRORS as e:
        print(f"Warning: Falling back to generated titles for '{dept_name}': {e}")

    # Top up with generated titles, reusing anything valid the model did manage to return.
    policies = fallback_policies(dept_name, domain, count, rng, exclude)
    have = {document_key(p["title"]) for p in policies}
    for extra in normalize_policies(data.get("policies"), count, rng, exclude):
        if len(policies) >= count:
            break
        key = document_key(extra["title"])
        if key not in have:
            have.add(key)
            policies.append(extra)
    return policies[:count]


def create_document(
    metadata: dict,
    company_name: str,
    company_id: str,
    dept_name: str,
    industry: str,
    created: str,
    updated: str,
) -> dict:
    """Builds the renderable document model from a policy's metadata.

    Seeds the generator on the document id so the same policy always produces the
    same body text, while different policies differ from each other.
    """
    return build_document(
        title=metadata["title"],
        company_name=company_name,
        company_id=company_id,
        dept_name=dept_name,
        industry=industry,
        version=metadata.get("version", "1.0"),
        audience=metadata.get("audience", "All employees"),
        privacy=metadata.get("privacy", "Internal"),
        doc_format=metadata.get("format", "pdf"),
        pages=metadata.get("pages", 7),
        created=created,
        updated=updated,
        seed=metadata["id"],
    )


# ==========================================
# Document Content + Rendering
# ==========================================

TABLE_WRAP_WIDTH = 96

SECTION_TITLES = (
    "Purpose and Objectives",
    "Scope and Applicability",
    "Definitions and Abbreviations",
    "Roles and Responsibilities",
    "Policy Statement",
    "Standard Operating Procedure",
    "Controls and Requirements",
    "Risk Assessment",
    "Escalation and Incident Handling",
    "Monitoring and Reporting",
    "Training and Awareness",
    "Records and Retention",
    "Compliance Monitoring",
    "Exceptions and Waivers",
    "Review and Revision History",
    "Supporting Notes",
)

_SUBJECT_NOISE = re.compile(
    r"\b(policies|policy|standards|standard|procedures|procedure|guidelines|guideline"
    r"|protocols|protocol|plans|plan|manuals|manual|notices|notice|charter)\b",
    re.IGNORECASE,
)

INDUSTRY_SUBJECTS = {
    "telecommunications": (
        "Product Catalog", "Roaming Agreement Register", "Tariff and Pricing Schedule",
        "Device Lifecycle Management", "Subscriber Offer Framework", "Wholesale Contract Templates",
        "Numbering and Porting", "Interconnect Billing", "Service Level Agreement",
        "Channel Partner Onboarding", "SIM and eSIM Provisioning", "Subscriber Complaint Handling",
    ),
    "technology": (
        "Release and Deployment Standard", "Service Level Objective Framework",
        "Data Processing Agreements", "Security Patch Management", "Capacity Planning",
        "Incident Postmortem Procedure", "Feature Flag Rollout", "Third-Party Dependency Upgrade",
        "Change Advisory Process", "Service Catalogue",
    ),
    "healthcare": (
        "Clinical Pathway Standard", "Patient Consent Procedure", "Care Plan Review",
        "Clinical Trial Protocol", "Medication Reconciliation", "Referral Handling",
        "Diagnostic Imaging Order", "Discharge Summary Requirements", "Infection Control",
    ),
    "financial services": (
        "Credit Assessment Framework", "Know Your Customer Review", "Account Opening Checklist",
        "Lending Decision Record", "Suspicious Activity Reporting", "Collateral Valuation",
        "Payment Instruction Controls", "Regulatory Capital Reporting",
    ),
    "logistics": (
        "Freight Booking Procedure", "Customs Declaration Standard", "Carrier Tender Process",
        "Last-Mile Delivery Protocol", "Cold-Chain Handover", "Shipment Exception Handling",
        "Warehouse Pick Wave Planning", "Return Authorisation", "Freight Rate Card",
    ),
    "renewable energy": (
        "Grid Connection Application", "Power Purchase Agreement", "Turbine Availability Claims",
        "Feed-In Tariff Submission", "Battery Dispatch Instructions", "Maintenance Outage Planning",
        "Meter Data Submission", "Carbon Accounting Entries", "Generation Forecasting",
    ),
    "manufacturing": (
        "Work Order Management", "Batch Release Procedure", "Material Traceability",
        "Machine Changeover Standard", "Supplier Corrective Action", "Calibration Certificates",
        "Non-Conformance Register", "Production Scheduling",
    ),
    "retail": (
        "Assortment Planogram", "Price Change Control", "Stock Replenishment Rules",
        "Returns Authorisation", "Promotion Mechanics", "Loyalty Tier Structure",
        "Shrinkage Investigation", "Markdown Policy",
    ),
    "education": (
        "Course Approval Procedure", "Student Enrolment Rules", "Assessment Marking Standards",
        "Academic Integrity Cases", "Bursary Application Process", "Safeguarding Referrals",
        "Credit Transfer Arrangements", "Timetable Allocation",
    ),
    "media and entertainment": (
        "Content Commissioning", "Rights Acquisition Register", "Distribution Windows",
        "Editorial Sign-Off", "Licence Renewal", "Audience Measurement",
        "Production Budget Control", "Release Scheduling",
    ),
    "agriculture": (
        "Crop Rotation Planning", "Input Application Records", "Harvest Booking",
        "Grading Standards", "Cold Storage Allocation", "Lot Traceability",
        "Irrigation Scheduling", "Livestock Health Checks",
    ),
    "construction": (
        "Site Instructions", "Change Order Procedure", "Method Statements",
        "Temporary Works Design", "Material Approval", "Snag List Management",
        "Permit to Work", "Progress Valuations",
    ),
    "hospitality": (
        "Room Allocation Rules", "Cancellation Policy", "Group Booking Procedures",
        "Housekeeping Schedules", "Allergen Declaration", "Maintenance Work Orders",
        "Guest Feedback Follow-Up", "Revenue Management Updates",
    ),
    "insurance": (
        "Claim Notification Procedure", "Underwriting Referrals", "Policy Endorsements",
        "Loss Adjuster Reports", "Fraud Indicators", "Reinsurance Cession",
        "Renewal Assessment", "Excess Determination",
    ),
    "pharmaceuticals": (
        "Batch Manufacturing Record", "GMP Deviation Handling", "Pharmacovigilance Reporting",
        "Marketing Authorisation Applications", "Batch Release Testing",
        "Cold Chain Excursions", "Distribution Agreements", "Product Recall Procedure",
    ),
}

INDUSTRY_VOCAB = {
    "telecommunications": {
        "terms": ("roaming partner agreement", "interconnect rate card", "subscriber activation",
                  "tariff change notice", "device subsidy scheme", "number portability request",
                  "wholesale billing cycle", "SIM provisioning request"),
        "artifacts": ("the roaming agreement register", "the published tariff catalogue",
                      "the device certification matrix"),
        "metrics": ("roaming attach rate", "port-out volume", "interconnect dispute volume",
                    "promotional offer redemption"),
    },
    "technology": {
        "terms": ("release candidate", "service level objective", "data processing agreement",
                  "security patch", "capacity request", "incident postmortem",
                  "feature flag rollout", "dependency upgrade"),
        "artifacts": ("the change advisory log", "the service catalogue", "the vulnerability register"),
        "metrics": ("change failure rate", "mean time to restore", "patch compliance", "on-call load"),
    },
    "healthcare": {
        "terms": ("clinical pathway", "patient consent record", "care plan review",
                  "clinical trial protocol", "medication reconciliation", "referral request",
                  "imaging order", "discharge summary"),
        "artifacts": ("the clinical guideline library", "the patient safety register",
                      "the consent audit trail"),
        "metrics": ("readmission rate", "consent completion rate", "care plan adherence",
                    "referral turnaround"),
    },
    "financial services": {
        "terms": ("credit application", "know-your-customer review", "account opening",
                  "lending decision", "suspicious activity report", "collateral valuation",
                  "payment instruction", "capital calculation"),
        "artifacts": ("the credit decision log", "the regulatory reporting pack",
                      "the suitability assessment"),
        "metrics": ("loss given default", "complaint resolution time", "kyc refresh backlog",
                    "capital adequacy"),
    },
    "logistics": {
        "terms": ("freight booking", "customs declaration", "carrier tender",
                  "last-mile delivery", "cold-chain handover", "shipment exception",
                  "warehouse pick wave", "return authorisation"),
        "artifacts": ("the carrier rate card", "the shipment tracking register",
                      "the customs classification file"),
        "metrics": ("on-time delivery rate", "damage claims ratio", "cost per shipment",
                    "dock-to-stock time"),
    },
    "renewable energy": {
        "terms": ("grid connection application", "power purchase agreement", "availability claim",
                  "feed-in tariff claim", "battery dispatch instruction", "maintenance outage",
                  "meter data submission", "carbon accounting entry"),
        "artifacts": ("the asset performance register", "the connection agreement file",
                      "the generation forecast log"),
        "metrics": ("capacity factor", "curtailment rate", "plant availability",
                    "levelised cost of energy"),
    },
    "manufacturing": {
        "terms": ("work order", "batch release", "material traceability record",
                  "machine changeover", "supplier corrective action", "calibration certificate",
                  "production lot", "downtime investigation"),
        "artifacts": ("the bill of materials", "the non-conformance register",
                      "the production schedule"),
        "metrics": ("first pass yield", "scrap rate", "unplanned downtime", "schedule adherence"),
    },
    "retail": {
        "terms": ("planogram reset", "price change", "stock replenishment", "returns authorisation",
                  "promotion mechanics", "loyalty tier change", "shrinkage investigation",
                  "supplier markdown"),
        "artifacts": ("the assortment planogram", "the pricing file", "the promotion calendar"),
        "metrics": ("sell-through rate", "stock turn", "markdown depth", "basket size"),
    },
    "education": {
        "terms": ("course approval", "student enrolment", "assessment marking",
                  "academic integrity case", "bursary application", "safeguarding referral",
                  "credit transfer", "timetable allocation"),
        "artifacts": ("the course catalogue", "the assessment rubric bank", "the enrolment register"),
        "metrics": ("completion rate", "grade distribution", "retention rate",
                    "student satisfaction"),
    },
    "media and entertainment": {
        "terms": ("content commissioning", "rights acquisition", "distribution window",
                  "editorial sign-off", "licence renewal", "audience measurement",
                  "production budget", "release schedule"),
        "artifacts": ("the rights ledger", "the content scheduling matrix",
                      "the commissioning register"),
        "metrics": ("audience reach", "engagement rate", "rights utilisation",
                    "production variance"),
    },
    "agriculture": {
        "terms": ("crop rotation plan", "input application record", "harvest booking",
                  "grading standard", "cold storage allocation", "traceability lot",
                  "irrigation schedule", "livestock health check"),
        "artifacts": ("the field activity log", "the harvest plan", "the traceability register"),
        "metrics": ("yield per hectare", "input cost per unit", "harvest loss rate",
                    "grading rejection rate"),
    },
    "construction": {
        "terms": ("site instruction", "change order", "method statement", "temporary works design",
                  "material approval", "snag list", "permit to work", "progress claim"),
        "artifacts": ("the site drawing register", "the change order log",
                      "the quality inspection plan"),
        "metrics": ("schedule variance", "rework cost", "safety observation rate",
                    "valuation accuracy"),
    },
    "hospitality": {
        "terms": ("room allocation", "cancellation policy", "group booking",
                  "housekeeping schedule", "allergen declaration", "maintenance work order",
                  "guest feedback follow-up", "revenue management update"),
        "artifacts": ("the reservation system rules", "the guest preference record",
                      "the outlet procedure manual"),
        "metrics": ("occupancy rate", "revenue per available room", "cancellation rate",
                    "guest satisfaction"),
    },
    "insurance": {
        "terms": ("claim notification", "underwriting referral", "policy endorsement",
                  "loss adjuster report", "fraud indicator", "reinsurance cession",
                  "renewal assessment", "excess determination"),
        "artifacts": ("the claims register", "the underwriting referral log",
                      "the policy wording library"),
        "metrics": ("loss ratio", "claims handling time", "renewal retention",
                    "fraud referral rate"),
    },
    "pharmaceuticals": {
        "terms": ("batch manufacturing record", "gmp deviation", "pharmacovigilance report",
                  "marketing authorisation application", "batch release testing",
                  "cold chain excursion", "distribution agreement", "product recall"),
        "artifacts": ("the batch documentation file", "the pharmacovigilance database",
                      "the registration dossier"),
        "metrics": ("batch rejection rate", "deviation closure time", "recall readiness",
                    "cold chain excursions"),
    },
}

GENERIC_VOCAB = {
    "terms": ("service request", "change request", "operational task", "customer commitment",
              "supplier engagement", "approval workflow"),
    "artifacts": ("the operational register", "the process record", "the control log"),
    "metrics": ("throughput", "backlog age", "first-time-right rate", "cost per unit"),
}

# An industry subject names the area; an aspect names the angle, so the same subject
# can yield several genuinely different documents ("Roaming Agreements - Disputes").
ASPECT_QUALIFIERS = (
    "Governance", "Roles and Responsibilities", "Approval Workflow", "Exception Handling",
    "Reporting and Escalation", "Audit and Evidence", "Service Levels", "Pricing and Billing",
    "Partner and Vendor Terms", "Onboarding", "Change Management", "Records and Retention",
    "Training and Awareness", "Quality Assurance", "Risk Controls", "Incident Management",
    "Customer Communication", "Performance Targets", "Data Quality", "Access and Permissions",
)


def industry_vocab(industry: str) -> dict:
    """Returns the domain vocabulary for an industry, falling back to generic terms."""
    return INDUSTRY_VOCAB.get(industry.strip().lower(), GENERIC_VOCAB)


def industry_subjects(industry: str) -> tuple:
    """Returns the document subjects typical of an industry."""
    return INDUSTRY_SUBJECTS.get(industry.strip().lower(), ())


SENTENCE_BANK = (
    "{dept} maintains this standard on behalf of {company} to keep {industry} operations defensible.",
    "Applies to all staff, contractors and automated systems that support the {subject} lifecycle.",
    "Ownership rests with the {dept} head, who may delegate day-to-day operation to a named owner.",
    "Every control described here is reviewed at least once per {period} by an independent reviewer.",
    "Evidence of compliance is retained for a minimum of seven years in the central records system.",
    "Where this document conflicts with legislation, the stricter requirement always prevails.",
    "Any deviation must be raised within one business day through the {subject} intake channel.",
    "Systems that process {subject} data must enforce encryption at rest and in transit.",
    "Access is granted on a least-privilege basis and recertified every {period}.",
    "Failures exceeding the {threshold} impact threshold trigger a formal incident record.",
    "Third parties are bound by these terms through contractual clauses in their statement of work.",
    "Training is mandatory before production access is granted to the {subject} environment.",
    "Metrics covering availability and error rate are published to stakeholders each {period}.",
    "Material changes to {industry} regulation are assessed for impact within {period}.",
    "The {role} is accountable for confirming that this document is followed in practice.",
    "Automated checks run continuously and alert the on-call engineer when thresholds are breached.",
    "Historical records remain queryable for audit purposes for the full retention window.",
    "Business continuity plans assume the {subject} capability can be restored within {period}.",
    "Exceptions require written approval from the {role} and are logged with a review date.",
    "Performance against the commitments in this document is reported to the {role} each {period}.",
    "Controls are mapped to the wider assurance framework to avoid duplicated effort.",
    "Personnel joining {dept} complete this training before receiving production credentials.",
    "Changes to {industry} practice are evaluated by the {dept} change advisory board.",
    "Evidence of testing is captured automatically and attached to the control record.",
    "Escalation proceeds to the {role} when the {threshold} threshold is not restored in time.",
    # Domain-specific: keep the prose anchored to the subject, not generic policy language.
    "Each {term} must be recorded in {artifact} before it is presented to a customer.",
    "Approval for any change to {artifact} is granted by the {role} and logged with a reference.",
    "The standing target for {metric} is agreed with the {role} and reported each {period}.",
    "Where {term} cannot be completed as described, the fallback path is recorded in {artifact}.",
    "Commercial terms relating to {term} may not be varied without written sign-off.",
    "Reconciliation of {metric} is performed before the {period} reporting cycle is closed.",
    "Any correction to {artifact} is versioned, with the superseded version retained for audit.",
    "Teams outside {dept} consume {artifact} read-only; changes are requested through the {role}.",
    "The {role} reviews {metric} alongside financial results to confirm the {term} assumptions hold.",
    "Customer-facing commitments made about {term} are traceable to a clause in {artifact}.",
    "Capacity to absorb {metric} is reassessed every {period} and signed off by the {role}.",
    "Data used to calculate {metric} is sourced from {system} and is not adjusted without approval.",
    "A {term} raised outside {dept} is handed to the {role} rather than handled locally.",
    "Clauses covering {term} are reviewed by {dept} before any renewal is negotiated.",
    "Corrections to {metric} are explained in the commentary that accompanies {artifact}.",
    "New starters shadow an experienced owner until they can process a {term} unaided.",
    "Where {term} is urgent, the shortened route is used and logged for review afterwards.",
    "The {role} may suspend {artifact} entries without notice where risk warrants it.",
    "Departments that consume {metric} are notified before the figures are restated.",
    "Templates for {term} are versioned centrally so that {dept} does not maintain local copies.",
    "Disputes about {term} are settled by reference to {artifact} and the recorded decision.",
)

ROLE_POOL = ("Control Owner", "Process Manager", "Service Owner", "Risk Lead", "Compliance Officer", "Data Steward")
SYSTEM_POOL = ("the asset inventory", "the configuration management database", "the change management log",
               "the monitoring stack", "the identity provider", "the ticketing platform")
PERIOD_POOL = ("quarter", "month", "six months", "year", "review period")
THRESHOLD_POOL = ("P1", "high", "critical", "red", "material")

TABLE_SPECS = (
    (("Control", "Owner", "Frequency"),
     ("Access recertification", "Service Owner", "Each {period}"),
     ("Change approval", "Process Manager", "Per change"),
     ("Backup restore test", "Control Owner", "Each {period}"),
     ("Log retention review", "Compliance Officer", "Each {period}")),
    (("Role", "Responsibility", "Escalation"),
     ("Control Owner", "Operates and evidences the control", "Process Manager"),
     ("Process Manager", "Approves changes and exceptions", "Risk Lead"),
     ("Risk Lead", "Maintains the risk register", "Compliance Officer"),
     ("Compliance Officer", "Reports to stakeholders", "Executive sponsor")),
    (("Stage", "Trigger", "Target"),
     ("Detection", "Automated alert or report", "{period} review"),
     ("Triage", "Initial impact assessment", "One business day"),
     ("Containment", "{threshold} impact confirmed", "Immediate"),
     ("Review", "Service restored", "Within {period}")),
    (("Metric", "Target", "Escalation if missed"),
     ("Availability", "99.5%", "Two consecutive {period}"),
     ("Change lead time", "Five business days", "Any P1 change"),
     ("Evidence completeness", "100%", "Any gap at audit"),
     ("Training completion", "100%", "Before access grant")),
)


def document_subject(title: str) -> str:
    """Strips generic policy words so 'Data Retention Policy' reads as 'Data Retention'."""
    stem = Path(title).stem
    cleaned = _SUBJECT_NOISE.sub("", stem)
    cleaned = re.sub(r"\s+", " ", cleaned).strip(" -_&")
    return cleaned or stem


def _paragraph(rng: random.Random, ctx: dict, bag: list) -> str:
    """Builds one paragraph by drawing distinct sentences from the bank.

    Sentences are taken from a shared bag that is reshuffled only when it runs low, so
    a template never repeats inside a paragraph and rarely repeats between two.
    """
    if len(bag) < 6:
        bag.clear()
        bag.extend(SENTENCE_BANK)
        rng.shuffle(bag)
    chosen = [bag.pop() for _ in range(min(rng.randint(4, 7), len(bag)))]
    return " ".join(template.format(**ctx) for template in chosen)


def _table(rng: random.Random, ctx: dict) -> dict:
    header, *rows = rng.choice(TABLE_SPECS)
    return {
        "header": [str(h) for h in header],
        "rows": [[str(c).format(**ctx) for c in row] for row in rows],
    }


def build_document(
    title: str,
    company_name: str,
    company_id: str,
    dept_name: str,
    industry: str,
    version: str,
    audience: str,
    privacy: str,
    doc_format: str,
    pages: int,
    created: str,
    updated: str,
    seed: str,
) -> dict:
    """Builds the canonical block model shared by every output format.

    Blocks are tuples: ("title", str) | ("meta", [(k, v)]) | ("h2", str)
    | ("p", str) | ("table", {"header": [...], "rows": [[...]]}).
    """
    rng = random.Random(seed)
    vocab = industry_vocab(industry)
    ctx = {
        "company": company_name,
        "dept": dept_name,
        "industry": industry,
        "subject": document_subject(title),
        "role": rng.choice(ROLE_POOL),
        "system": rng.choice(SYSTEM_POOL),
        "period": rng.choice(PERIOD_POOL),
        "threshold": rng.choice(THRESHOLD_POOL),
        "term": rng.choice(vocab["terms"]),
        "artifact": rng.choice(vocab["artifacts"]),
        "metric": rng.choice(vocab["metrics"]),
    }

    meta = [
        ("Document Name", title),
        ("Company", company_name),
        ("Company ID", company_id),
        ("Department", dept_name),
        ("Industry", industry),
        ("Version", version),
        ("Audience", audience),
        ("Privacy Level", privacy),
        ("Date of Creation", created),
        ("Date of Update", updated),
        ("Format", doc_format.upper()),
        ("Estimated Pages", str(pages)),
    ]

    blocks: list = [("title", title), ("subtitle", f"{company_name} · {dept_name}"), ("meta", meta)]
    headings = list(SECTION_TITLES)
    rng.shuffle(headings)
    index = 0
    words = 0
    tables = 0

    # Grows the document paragraph by paragraph, tracking the real paginator so the
    # finished file lands on exactly `pages` and is never allowed to run away.
    bag: list = []
    ceiling = pages * PAGE_UNITS + PAGE_UNITS
    while _count_pages(blocks) < pages and _page_units(blocks) < ceiling:
        heading = headings[index % len(headings)]
        if index >= len(headings):
            heading = f"{heading} (continued {index // len(headings) + 1})"
        blocks.append(("h2", heading))

        for _ in range(rng.randint(2, 3)):
            if _count_pages(blocks) >= pages:
                break
            # Roll the domain terms per paragraph: repeating a template then reads
            # differently each time instead of verbatim.
            ctx["term"] = rng.choice(vocab["terms"])
            ctx["artifact"] = rng.choice(vocab["artifacts"])
            ctx["metric"] = rng.choice(vocab["metrics"])
            para = _paragraph(rng, ctx, bag)
            if _page_units(blocks) + _paragraph_units(para) > ceiling:
                break
            blocks.append(("p", para))
            words += len(para.split())

        if rng.random() < 0.45 and _page_units(blocks) < ceiling:
            table = _table(rng, ctx)
            if _page_units(blocks) + _table_units(table) <= ceiling:
                blocks.append(("table", table))
                tables += 1

        index += 1

    return {
        "name": title,
        "version": version,
        "audience": audience,
        "privacy": privacy,
        "created": created,
        "updated": updated,
        "format": doc_format,
        "pages": pages,
        "words": words,
        "tables": tables,
        "blocks": blocks,
    }


# ---------- shared helpers ----------

def _wrap(text: str, width: int) -> list:
    lines: list = []
    current = ""
    for word in text.split():
        if current and len(current) + 1 + len(word) <= width:
            current = f"{current} {word}"
        else:
            if current:
                lines.append(current)
            current = word
    if current:
        lines.append(current)
    return lines or [""]


def _flat_table(table: dict, width: int = TABLE_WRAP_WIDTH) -> list:
    """Renders a table as fixed-width text lines."""
    header = table["header"]
    cols = len(header)
    col_w = max(10, width // cols)
    rule = "-" * (col_w * cols + 2 * (cols - 1))

    def row(cells):
        cells = list(cells) + [""] * (cols - len(cells))
        return "  ".join(str(c)[:col_w].ljust(col_w) for c in cells).rstrip()

    lines = [row(header), rule]
    lines.extend(row(r) for r in table["rows"])
    lines.append(rule)
    return lines


def _page_units(blocks: list) -> int:
    """Measures rendered height in points, mirroring the PDF layout engine.

    A US Letter page with 54pt margins gives 684 usable points, so a 14pt line
    consumes one unit of 14 and the page fills at 684.
    """
    units = 0
    for kind, payload in blocks:
        if kind == "title":
            units += 18 + 12 + 14
        elif kind == "subtitle":
            units += 18 + 14
        elif kind == "meta":
            units += len(payload) * 14 + 12 + 14
        elif kind == "h2":
            units += 14 + 18 + 12
        elif kind == "p":
            units += len(_wrap(payload, 88)) * 14 + 14
        elif kind == "table":
            units += len(_flat_table(payload, 88)) * 14 + 28
    return units


PAGE_UNITS = 684


def _paragraph_units(text: str) -> int:
    """Height a paragraph will occupy, matching the PDF line wrapping."""
    return len(_wrap(text, 88)) * 14 + 14


def _table_units(table: dict) -> int:
    return len(_flat_table(table, 88)) * 14 + 28


def _count_pages(blocks: list) -> int:
    """Counts rendered pages using the same paginator the PDF renderer draws from."""
    return len(_paginate(blocks))


# ---------- format writers ----------

def render_text(doc: dict) -> str:
    out: list = []
    for kind, payload in doc["blocks"]:
        if kind == "meta":
            out.append("-" * TABLE_WRAP_WIDTH)
            for key, value in payload:
                out.append(f"{key + ':':<22}{value}")
            out.append("-" * TABLE_WRAP_WIDTH)
        elif kind == "title":
            out.extend([payload.upper(), "=" * len(payload), ""])
        elif kind == "subtitle":
            out.extend([payload, ""])
        elif kind == "h2":
            out.extend(["", payload, "-" * len(payload)])
        elif kind == "p":
            out.extend(_wrap(payload, TABLE_WRAP_WIDTH))
            out.append("")
        elif kind == "table":
            out.extend(_flat_table(payload))
            out.append("")
    return "\n".join(out).rstrip() + "\n"


def render_markdown(doc: dict) -> str:
    subtitle = next((p for k, p in doc["blocks"] if k == "subtitle"), "")
    meta = next((p for k, p in doc["blocks"] if k == "meta"), [])
    out: list = [f"# {doc['name']}", ""]
    if subtitle:
        out.extend([f"*{subtitle}*", ""])
    out.append("| Field | Value |")
    out.append("| --- | --- |")
    for key, value in meta:
        out.append(f"| {key} | {value} |")
    out.append("")
    for kind, payload in doc["blocks"]:
        if kind == "h2":
            out.extend([f"## {payload}", ""])
        elif kind == "p":
            out.extend([payload, ""])
        elif kind == "table":
            out.append("| " + " | ".join(payload["header"]) + " |")
            out.append("| " + " | ".join("---" for _ in payload["header"]) + " |")
            for row in payload["rows"]:
                out.append("| " + " | ".join(str(c) for c in row) + " |")
            out.append("")
    return "\n".join(out).rstrip() + "\n"


def _esc(text: str) -> str:
    return (
        str(text).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace('"', "&quot;")
    )


def render_html(doc: dict) -> str:
    parts: list = [
        "<!DOCTYPE html>",
        '<html lang="en">',
        "<head>",
        '<meta charset="utf-8">',
        f"<title>{_esc(doc['name'])}</title>",
        "<style>",
        "body{font-family:Georgia,serif;max-width:46em;margin:2.5em auto;padding:0 1.5em;line-height:1.6}",
        "h1{border-bottom:3px solid #222;padding-bottom:.3em}",
        ".subtitle{color:#555;font-style:italic;margin:-.4em 0 1.4em}",
        "h2{margin-top:1.8em;border-bottom:1px solid #ccc;padding-bottom:.2em}",
        "table{border-collapse:collapse;width:100%;margin:1.2em 0}",
        "th,td{border:1px solid #999;padding:.45em .6em;text-align:left;font-size:.92em}",
        "th{background:#eee}",
        ".meta td:first-child{width:32%;font-weight:bold}",
        "@media print{h2{page-break-after:avoid}table{page-break-inside:avoid}}",
        "</style>",
        "</head>",
        "<body>",
        f"<h1>{_esc(doc['name'])}</h1>",
    ]
    subtitle = next((p for k, p in doc["blocks"] if k == "subtitle"), "")
    if subtitle:
        parts.append(f'<p class="subtitle">{_esc(subtitle)}</p>')
    for kind, payload in doc["blocks"]:
        if kind == "meta":
            parts.append('<table class="meta">')
            for key, value in payload:
                parts.append(f"<tr><td>{_esc(key)}</td><td>{_esc(value)}</td></tr>")
            parts.append("</table>")
        elif kind == "h2":
            parts.append(f"<h2>{_esc(payload)}</h2>")
        elif kind == "p":
            parts.append(f"<p>{_esc(payload)}</p>")
        elif kind == "table":
            parts.append("<table>")
            parts.append("<tr>" + "".join(f"<th>{_esc(h)}</th>" for h in payload["header"]) + "</tr>")
            for row in payload["rows"]:
                parts.append("<tr>" + "".join(f"<td>{_esc(c)}</td>" for c in row) + "</tr>")
            parts.append("</table>")
    parts.extend(["</body>", "</html>"])
    return "\n".join(parts) + "\n"


def _xml(text: str) -> str:
    return (
        str(text).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace('"', "&quot;")
    )


def render_docx(doc: dict) -> bytes:
    """Builds a real .docx (OOXML package) using only the standard library."""
    import io
    import zipfile

    body: list = []
    for kind, payload in doc["blocks"]:
        if kind == "title":
            body.append(
                '<w:p><w:pPr><w:pStyle w:val="Title"/></w:pPr>'
                f'<w:r><w:t xml:space="preserve">{_xml(payload)}</w:t></w:r></w:p>'
            )
        elif kind == "subtitle":
            body.append(
                '<w:p><w:pPr><w:pStyle w:val="Subtitle"/></w:pPr>'
                f'<w:r><w:t xml:space="preserve">{_xml(payload)}</w:t></w:r></w:p>'
            )
        elif kind == "h2":
            body.append(
                '<w:p><w:pPr><w:pStyle w:val="Heading1"/></w:pPr>'
                f'<w:r><w:t xml:space="preserve">{_xml(payload)}</w:t></w:r></w:p>'
            )
        elif kind == "meta":
            for key, value in payload:
                body.append(
                    "<w:p><w:r><w:rPr><w:b/></w:rPr>"
                    f'<w:t xml:space="preserve">{_xml(key)}: </w:t></w:r>'
                    f'<w:r><w:t xml:space="preserve">{_xml(value)}</w:t></w:r></w:p>'
                )
            body.append("<w:p/>")
        elif kind == "p":
            body.append(f'<w:p><w:r><w:t xml:space="preserve">{_xml(payload)}</w:t></w:r></w:p>')
        elif kind == "table":
            cells = "".join(
                f'<w:tc><w:tcPr><w:shd w:val="clear" w:fill="EEEEEE"/></w:tcPr>'
                f'<w:p><w:r><w:rPr><w:b/></w:rPr><w:t xml:space="preserve">{_xml(h)}</w:t></w:r></w:p></w:tc>'
                for h in payload["header"]
            )
            rows = [f"<w:tr>{cells}</w:tr>"]
            for row in payload["rows"]:
                tcs = "".join(
                    f'<w:tc><w:p><w:r><w:t xml:space="preserve">{_xml(c)}</w:t></w:r></w:p></w:tc>'
                    for c in row
                )
                rows.append(f"<w:tr>{tcs}</w:tr>")
            body.append(
                "<w:tbl><w:tblPr><w:tblStyle w:val=\"TableGrid\"/>"
                '<w:tblW w:w="5000" w:type="pct"/></w:tblPr>' + "".join(rows) + "</w:tbl>"
            )
            body.append("<w:p/>")

    document_xml = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<w:document xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main">'
        f"<w:body>{''.join(body)}"
        '<w:sectPr><w:pgSz w:w="12240" w:h="15840"/>'
        '<w:pgMar w:top="1440" w:right="1440" w:bottom="1440" w:left="1440"/></w:sectPr>'
        "</w:body></w:document>"
    )

    content_types = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">'
        '<Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>'
        '<Default Extension="xml" ContentType="application/xml"/>'
        '<Override PartName="/word/document.xml" ContentType="application/vnd.openxmlformats-officedocument'
        '.wordprocessingml.document.main+xml"/>'
        '<Override PartName="/word/styles.xml" ContentType="application/vnd.openxmlformats-officedocument'
        '.wordprocessingml.styles+xml"/>'
        "</Types>"
    )

    root_rels = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
        '<Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships'
        '/officeDocument" Target="word/document.xml"/>'
        "</Relationships>"
    )

    doc_rels = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
        '<Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships'
        '/styles" Target="styles.xml"/>'
        "</Relationships>"
    )

    styles = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<w:styles xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main">'
        '<w:style w:type="paragraph" w:styleId="Title"><w:name w:val="Title"/>'
        '<w:rPr><w:b/><w:sz w:val="48"/></w:rPr></w:style>'
        '<w:style w:type="paragraph" w:styleId="Subtitle"><w:name w:val="Subtitle"/>'
        '<w:rPr><w:i/><w:color w:val="555555"/><w:sz w:val="24"/></w:rPr></w:style>'
        '<w:style w:type="paragraph" w:styleId="Heading1"><w:name w:val="heading 1"/>'
        '<w:rPr><w:b/><w:sz w:val="28"/></w:rPr></w:style>'
        '<w:style w:type="table" w:styleId="TableGrid"><w:name w:val="Table Grid"/>'
        "<w:tblPr><w:tblBorders>"
        '<w:top w:val="single" w:sz="4" w:color="999999"/>'
        '<w:left w:val="single" w:sz="4" w:color="999999"/>'
        '<w:bottom w:val="single" w:sz="4" w:color="999999"/>'
        '<w:right w:val="single" w:sz="4" w:color="999999"/>'
        '<w:insideH w:val="single" w:sz="4" w:color="999999"/>'
        '<w:insideV w:val="single" w:sz="4" w:color="999999"/>'
        "</w:tblBorders></w:tblPr></w:style>"
        "</w:styles>"
    )

    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("[Content_Types].xml", content_types)
        zf.writestr("_rels/.rels", root_rels)
        zf.writestr("word/_rels/document.xml.rels", doc_rels)
        zf.writestr("word/document.xml", document_xml)
        zf.writestr("word/styles.xml", styles)
    return buffer.getvalue()


def _pdf_escape(text: str) -> str:
    return text.replace("\\", r"\\").replace("(", r"\(").replace(")", r"\)")


def _pdf_flatten(blocks: list) -> list:
    """Turns blocks into a flat list of (kind, text, size, bold) drawing ops."""
    ops: list = []

    def add(kind, text="", size=11, bold=False):
        ops.append((kind, text, size, bold))

    for kind, payload in blocks:
        if kind == "title":
            add("text", payload, 18, True)
            add("rule")
        elif kind == "subtitle":
            add("text", payload, 12, False)
            add("space")
        if kind == "title":
            add("space")
        elif kind == "meta":
            for key, value in payload:
                add("meta", f"{key}: {value}", 10, False)
            add("rule")
            add("space")
        elif kind == "h2":
            add("space")
            add("text", payload, 13, True)
            add("rule")
        elif kind == "p":
            for line in _wrap(payload, 88):
                add("text", line, 11, False)
            add("space")
        elif kind == "table":
            add("space")
            for line in _flat_table(payload, 88):
                add("text", line, 9, False)
            add("space")
    return ops


PAGE_WIDTH, PAGE_HEIGHT = 612, 792
PAGE_MARGIN, LEADING = 54, 14


def _paginate(blocks: list) -> list:
    """Lays ops out into pages, breaking lines that no longer fit.

    This is the single source of truth for page count: render_pdf draws it and
    _count_pages measures it, so the two can never disagree.
    """
    bottom = PAGE_MARGIN
    pages: list = [[]]
    y = PAGE_HEIGHT - PAGE_MARGIN

    for kind, text, size, bold in _pdf_flatten(blocks):
        if kind == "space":
            y -= LEADING
            continue
        if kind == "rule":
            if y < bottom + LEADING:
                pages.append([])
                y = PAGE_HEIGHT - PAGE_MARGIN
            y -= 6
            pages[-1].append(("rule", y))
            y -= 6
            continue
        line_h = LEADING if size <= 11 else LEADING + 4
        if y - line_h < bottom:
            pages.append([])
            y = PAGE_HEIGHT - PAGE_MARGIN
        y -= line_h
        pages[-1].append(("text", text, y, size, bold))

    return [p for p in pages if p] or [[]]


def render_pdf(doc: dict) -> bytes:
    """Writes a real, standards-compliant PDF using only the standard library."""
    page_w, page_h = PAGE_WIDTH, PAGE_HEIGHT
    margin = PAGE_MARGIN

    streams: list = []
    for page in _paginate(doc["blocks"]):
        parts: list = []
        for item in page:
            if item[0] == "rule":
                _, ry = item
                parts.append(
                    f"0.82 G 0.5 w {margin} {ry:.1f} m {page_w - margin} {ry:.1f} l S"
                )
            else:
                _, text, ty, size, bold = item
                font = "F2" if bold else "F1"
                parts.append(
                    f"BT /{font} {size} Tf 1 0 0 1 {margin} {ty:.1f} Tm "
                    f"({_pdf_escape(text)}) Tj ET"
                )
        streams.append("\n".join(parts).encode("latin-1", "replace"))

    page_count = len(streams)
    first_page_obj = 3
    regular_font = first_page_obj + 2 * page_count
    bold_font = regular_font + 1

    table: dict = {}
    kids = " ".join(f"{first_page_obj + 2 * i} 0 R" for i in range(page_count))
    table[1] = b"<< /Type /Catalog /Pages 2 0 R >>"
    table[2] = f"<< /Type /Pages /Kids [{kids}] /Count {page_count} >>".encode()

    for index, stream in enumerate(streams):
        page_obj = first_page_obj + 2 * index
        content_obj = page_obj + 1
        table[page_obj] = (
            f"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 {page_w} {page_h}] "
            f"/Resources << /Font << /F1 {regular_font} 0 R /F2 {bold_font} 0 R >> >> "
            f"/Contents {content_obj} 0 R >>"
        ).encode()
        table[content_obj] = (
            b"<< /Length " + str(len(stream)).encode() + b" >>\nstream\n" + stream + b"\nendstream"
        )

    table[regular_font] = (
        b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica /Encoding /WinAnsiEncoding >>"
    )
    table[bold_font] = (
        b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica-Bold /Encoding /WinAnsiEncoding >>"
    )

    out = bytearray(b"%PDF-1.4\n")
    offsets: dict = {}
    for number in sorted(table):
        offsets[number] = len(out)
        out += f"{number} 0 obj\n".encode() + table[number] + b"\nendobj\n"

    size = max(table) + 1
    xref_at = len(out)
    out += f"xref\n0 {size}\n".encode()
    out += b"0000000000 65535 f \n"
    for number in range(1, size):
        out += f"{offsets[number]:010d} 00000 n \n".encode()
    out += f"trailer\n<< /Size {size} /Root 1 0 R >>\nstartxref\n{xref_at}\n%%EOF\n".encode()

    return bytes(out)


RENDERERS = {
    "pdf": render_pdf,
    "html": render_html,
    "docx": render_docx,
    "markdown": render_markdown,
    "text": render_text,
}


def render_document(doc: dict) -> bytes:
    """Renders the document model in its declared format."""
    result = RENDERERS[doc["format"]](doc)
    return result if isinstance(result, bytes) else result.encode("utf-8")


def parse_args(argv: list | None = None) -> argparse.Namespace:
    """Parses the command line.

    --industry     create a new company in that industry
    --company-id   append new documents to a company that already exists
    (neither)      create a new company in a randomly chosen industry
    """
    parser = argparse.ArgumentParser(
        prog="docgen.py",
        description="Generates company departments and policy documents on disk.",
    )
    parser.add_argument(
        "--industry",
        default="",
        help="Create a new company in this industry, e.g. --industry='Renewable Energy'.",
    )
    parser.add_argument(
        "--company-id",
        "--company_id",
        dest="company_id",
        default="",
        help="Append documents to an existing company id instead of creating a new one.",
    )
    parser.add_argument(
        "--fresh",
        action="store_true",
        help="Delete the state file before running, starting from an empty state.",
    )
    parser.add_argument(
        "--max-docs",
        type=int,
        default=10,
        help="Maximum documents a department may hold (default 10, the top of the 5-10 range).",
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="Print the known companies from the state file and exit.",
    )
    return parser.parse_args(argv)


# ==========================================
# State Management Class
# ==========================================

class StateManager:
    def __init__(self, filepath=STATE_FILE):
        self.filepath = filepath
        self.state = self.load_state()

    def load_state(self) -> dict:
        """Loads state from disk if available, otherwise initializes dynamic structure."""
        if os.path.exists(self.filepath):
            try:
                with open(self.filepath, "r", encoding="utf-8") as f:
                    data = json.load(f)
            except json.JSONDecodeError:
                print("Warning: State file corrupted. Initializing new state.")
            else:
                companies = data.get("companies") if isinstance(data, dict) else None
                if isinstance(companies, list):
                    return {"companies": companies}
                print("Warning: State file has an unexpected shape. Initializing new state.")
        return {"companies": []}

    def save_state(self):
        """Persists current state back to the JSON file."""
        with open(self.filepath, "w", encoding="utf-8") as f:
            json.dump(self.state, f, indent=2)

    def add_company(self, company_id: str, name: str, industry: str) -> bool:
        """Appends a new company if it doesn't already exist."""
        if self._find_company(company_id):
            print(f"Error: Company '{company_id}' already exists.")
            return False

        new_company = {
            "id": company_id,
            "name": name,
            "industry": industry,
            "departments": [],
        }
        self.state["companies"].append(new_company)
        self.save_state()
        print(f"Added company: {name} ({company_id})")
        return True

    def add_department(self, company_id: str, dept_id: str, dept_name: str) -> bool:
        """Adds a department to an existing company."""
        company = self._find_company(company_id)
        if not company:
            print(f"Error: Company '{company_id}' not found.")
            return False

        if self._find_department(company, dept_id):
            print(f"Error: Department '{dept_id}' already exists in {company_id}.")
            return False

        new_dept = {
            "id": dept_id,
            "name": dept_name,
            "documents": [],
        }
        company["departments"].append(new_dept)
        self.save_state()
        print(f"Added department '{dept_name}' to company '{company_id}'.")
        return True

    def add_document(
        self,
        company_id: str,
        dept_id: str,
        doc_id: str,
        title: str,
        doc_format: str,
        version: str = "1.0",
        audience: str = "",
        privacy: str = "",
        pages: int = 0,
    ) -> bool:
        """Adds a document to a specific department in a company."""
        company = self._find_company(company_id)
        if not company:
            print(f"Error: Company '{company_id}' not found.")
            return False

        dept = self._find_department(company, dept_id)
        if not dept:
            print(f"Error: Department '{dept_id}' not found in company '{company_id}'.")
            return False

        if any(d["id"] == doc_id for d in dept["documents"]):
            print(f"Error: Document '{doc_id}' already exists in department '{dept_id}'.")
            return False

        now = datetime.now(UTC)
        new_doc = {
            "id": doc_id,
            "title": title,
            "format": doc_format,
            "type": doc_format.upper(),
            "version": version,
            "audience": audience,
            "privacy": privacy,
            "pages": pages,
            "file": f"{DATA_ROOT}/{company_id}/{dept_id}/{doc_id}",
            "created_at": now.strftime("%Y-%m-%d"),
            "updated_at": now.strftime("%Y-%m-%d"),
        }

        dept["documents"].append(new_doc)
        self.save_state()
        print(f"Added document '{title}' to department '{dept_id}'.")
        return True

    def _find_company(self, company_id: str):
        return next((c for c in self.state["companies"] if c["id"] == company_id), None)

    def get_company(self, company_id: str) -> dict | None:
        """Returns a company by id, or None when it is not in the state file."""
        return self._find_company(company_id)

    def company_ids(self) -> list:
        """Returns every known company id, in the order they were added."""
        return [c["id"] for c in self.state["companies"] if isinstance(c, dict) and c.get("id")]

    def department_titles(self, company_id: str) -> dict:
        """Maps department id to the set of document ids it already holds."""
        company = self._find_company(company_id)
        if not company:
            return {}
        return {
            dept["id"]: {doc["id"] for doc in dept.get("documents", [])}
            for dept in company.get("departments", [])
        }

    def _find_department(self, company: dict, dept_id: str):
        return next((d for d in company["departments"] if d["id"] == dept_id), None)

    def display(self):
        """Prints current JSON state."""
        print(json.dumps(self.state, indent=2))


# ==========================================
# Execution Loop
# ==========================================

if __name__ == "__main__":
    args = parse_args()

    # --fresh clears the previous state before it is loaded.
    if args.fresh and os.path.exists(STATE_FILE):
        os.remove(STATE_FILE)
        print(f"Removed state file: {STATE_FILE}")

    # State is read (or initialised) before anything is generated, so we always know
    # which companies already exist before deciding to create or extend one.
    manager = StateManager(STATE_FILE)
    manager.save_state()
    known = manager.company_ids()
    print(f"State: {STATE_FILE} ({len(known)} existing company/companies)")

    if args.list:
        if not known:
            print("No companies in state yet.")
        for company in manager.state["companies"]:
            depts = company.get("departments", [])
            docs = sum(len(d.get("documents", [])) for d in depts)
            print(
                f"  {company['id']:<18} {company.get('name', ''):<36} "
                f"{company.get('industry', ''):<18} {len(depts)} dept(s), {docs} doc(s)"
            )
        sys.exit(0)

    # --company-id extends a company that is already on disk; --industry creates a new one.
    if args.company_id:
        company = manager.get_company(args.company_id)
        if not company:
            print(f"Error: company '{args.company_id}' not found in {STATE_FILE}.")
            print(f"Known company ids: {', '.join(known) if known else '(none yet)'}")
            sys.exit(1)

        company_id = company["id"]
        company_name = company.get("name", company_id)
        industry_domain = company.get("industry") or resolve_industry()
        print(f"Appending to existing company: {company_name} ({company_id}) [{industry_domain}]")
        departments = [d["name"] for d in company.get("departments", [])]
        if not departments:
            print("Company has no departments yet; generating them now.")
            departments = generate_departments(industry_domain, company_name=company_name)
    else:
        industry_domain = resolve_industry(args.industry)
        print(f"Industry: {industry_domain}")

        identity = generate_company_name_and_id(industry_domain)
        company_name = identity.get("company_name") or "Acme Corporation"
        company_id = identity.get("company_id") or "comp-001"
        if identity.get("warning"):
            print(f"Warning: {identity['warning']}")

        if not manager.add_company(company_id, company_name, industry_domain):
            # The generated id is already taken, so extend that company instead.
            existing = manager.get_company(company_id) or {}
            print(
                f"Company '{company_id}' already exists; adding to it instead of creating a duplicate."
            )
            company_name = existing.get("name", company_name)
            industry_domain = existing.get("industry", industry_domain)
            departments = [d["name"] for d in existing.get("departments", [])]
        else:
            print(f"New company: {company_name} ({company_id})")
            departments = generate_departments(industry_domain, company_name=company_name)

    # Each department gets a different document count, all within 5-10.
    counts = distinct_counts(len(departments))
    existing_docs = manager.department_titles(company_id)
    # Seed with every title the company already has, across all departments, and
    # extend it as the run goes. Otherwise a department that is already at its cap
    # contributes no titles, and a later department re-issues the same documents.
    run_titles: set = {document_key(doc) for ids in existing_docs.values() for doc in ids}

    # Build and render one file per document, skipping ids already on disk.
    today = datetime.now(UTC).strftime("%Y-%m-%d")
    written_files = 0
    added_departments: list = []
    for dept_name, doc_count in zip(departments, counts, strict=False):
        dept_id = slugify(dept_name)
        if not manager.add_department(company_id, dept_id, dept_name):
            existing = manager.get_company(company_id) or {}
            dept = next(
                (d for d in existing.get("departments", []) if d["id"] == dept_id), None
            )
            if not dept:
                continue
            print(f"Department '{dept_id}' already exists; adding documents to it.")
        else:
            added_departments.append(dept_name)

        # Never push a department past --max-docs, so re-running stays inside 5-10.
        current = manager.get_company(company_id) or {}
        held = next(
            (d for d in current.get("departments", []) if d["id"] == dept_id), {}
        ).get("documents", [])
        headroom = max(0, args.max_docs - len(held))
        if headroom == 0:
            print(f"Department '{dept_id}' already holds {len(held)} documents; skipping.")
            continue
        batch = min(doc_count, headroom)
        if batch < doc_count:
            print(
                f"Department '{dept_id}' holds {len(held)}/{args.max_docs}; "
                f"adding {batch} instead of {doc_count}."
            )

        policies = generate_department_policies(
            domain=industry_domain,
            dept_name=dept_name,
            company_name=company_name,
            count=batch,
            exclude=run_titles | existing_docs.get(dept_id, set()),
        )
        run_titles.update(document_key(p["title"]) for p in policies)

        for policy in policies:
            if not manager.add_document(
                company_id=company_id,
                dept_id=dept_id,
                doc_id=policy["id"],
                title=policy["title"],
                doc_format=policy["format"],
                version=policy["version"],
                audience=policy["audience"],
                privacy=policy["privacy"],
                pages=policy["pages"],
            ):
                continue

            doc = create_document(
                policy,
                company_name=company_name,
                company_id=company_id,
                dept_name=dept_name,
                industry=industry_domain,
                created=today,
                updated=today,
            )
            payload = render_document(doc)
            path = write_document(company_id, dept_id, policy["id"], payload)
            written_files += 1
            print(
                f"Wrote {path} "
                f"({policy['format']}, {doc['pages']}p, {doc['words']} words, {len(payload)} bytes)"
            )

    # Mirror the directories that actually made it into state.
    if added_departments:
        company_path = create_company_directories(company_id, added_departments)
        print(f"Created company directories: {company_path}/ ({', '.join(added_departments)})")

    print(f"Generated {written_files} document file(s) on disk.")
    print("\n--- Final State Output ---")
    manager.display()
