/**
 * SubjectCard — top-of-page summary of the LEI lookup subject.
 */
export function SubjectCard({ lei, legalName }: { lei: string; legalName: string | null }) {
  return (
    <section className="mb-8 bg-white border border-oo-rule rounded-oo p-7 transition-shadow hover:shadow-oo-card">
      <p className="text-[11px] font-semibold tracking-oo-eyebrow uppercase text-oo-blue">
        Subject
      </p>
      <h2 className="font-head font-bold text-oo-ink mt-2 leading-tight text-[clamp(1.25rem,2.5vw,1.6rem)]">
        {legalName || `LEI ${lei}`}
      </h2>
    </section>
  );
}
